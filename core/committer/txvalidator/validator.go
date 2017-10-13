/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

                 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package txvalidator

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/flogging"
	coreUtil "github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/common/sysccprovider"
	"github.com/hyperledger/fabric/core/common/validation"
	"github.com/hyperledger/fabric/core/ledger"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/op/go-logging"

	"github.com/hyperledger/fabric/common/cauthdsl"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
)

// number of simultaneous vscc executions allowed
var parallelVSCCWorkerCount = 2 //runtime.NumCPU()

var txvalidator_log, _ = os.Create("/root/txvalidator.log")

// Support provides all of the needed to evaluate the VSCC
type Support interface {
	// Ledger returns the ledger associated with this validator
	Ledger() ledger.PeerLedger

	// MSPManager returns the MSP manager for this chain
	MSPManager() msp.MSPManager

	// Apply attempts to apply a configtx to become the new config
	Apply(configtx *common.ConfigEnvelope) error

	// GetMSPIDs returns the IDs for the application MSPs
	// that have been defined in the channel
	GetMSPIDs(cid string) []string
}

//Validator interface which defines API to validate block transactions
// and return the bit array mask indicating invalid transactions which
// didn't pass validation.
type Validator interface {
	Validate(block *common.Block) error
}

// private interface to decouple tx validator
// and vscc execution, in order to increase
// testability of txValidator
type vsccValidator interface {
	VSCCValidateTx(payload *common.Payload, envBytes []byte, env *common.Envelope) (error, peer.TxValidationCode)
}

type cDataMap struct {
	sync.RWMutex
	m map[string]*ccprovider.ChaincodeData
}

// vsccValidator implementation which used to call
// vscc chaincode and validate block transactions
type vsccValidatorImpl struct {
	support     Support
	ccprovider  ccprovider.ChaincodeProvider
	sccprovider sysccprovider.SystemChaincodeProvider
	cDataMap    *cDataMap
}

// implementation of Validator interface, keeps
// reference to the ledger to enable tx simulation
// and execution of vscc
type txValidator struct {
	support         Support
	vscc            vsccValidator
	cDataMap        *cDataMap
	vsccWorkerToken chan struct{}
}

var logger *logging.Logger // package-level logger
var vsccWorkerToken chan struct{}

func init() {
	// Init logger with module name
	logger = flogging.MustGetLogger("txvalidator")

	var f, _ = os.Open("/root/parallelVSCCWorkerCount.txt")
	fmt.Fscanf(f, "%d", &parallelVSCCWorkerCount)

	// fill up the worker tokens
	vsccWorkerToken = make(chan struct{}, parallelVSCCWorkerCount)
	for i := 0; i < parallelVSCCWorkerCount; i++ {
		vsccWorkerToken <- struct{}{}
	}
	txvalidator_log.WriteString(fmt.Sprintf("Added %d vscc worker tokens\n", parallelVSCCWorkerCount))

}

// NewTxValidator creates new transactions validator
func NewTxValidator(support Support) Validator {
	// Encapsulates interface implementation

	cDataMap := &cDataMap{
		m: make(map[string]*ccprovider.ChaincodeData),
	}

	var f, _ = os.Open("/root/parallelVSCCWorkerCount.txt")
	fmt.Fscanf(f, "%d", &parallelVSCCWorkerCount)

	return &txValidator{support,
		&vsccValidatorImpl{
			support:     support,
			ccprovider:  ccprovider.GetChaincodeProvider(),
			sccprovider: sysccprovider.GetSystemChaincodeProvider(),
			cDataMap:    cDataMap},
		cDataMap,
		make(chan struct{}, parallelVSCCWorkerCount)}
}

func (v *txValidator) chainExists(chain string) bool {
	// TODO: implement this function!
	return true
}

func (v *txValidator) parallelVSCCValidateTx(block *common.Block, tIdx int, d []byte, env *common.Envelope) (peer.TxValidationCode,
	*sysccprovider.ChaincodeInstance, *sysccprovider.ChaincodeInstance, error) {
	// validate the transaction: here we check that the transaction
	// is properly formed, properly signed and that the security
	// chain binding proposal to endorsements to tx holds. We do
	// NOT check the validity of endorsements, though. That's a
	// job for VSCC below
	logger.Debug("Validating transaction peer.ValidateTransaction()")
	var payload *common.Payload
	var err error
	var txResult peer.TxValidationCode
	var ccName *sysccprovider.ChaincodeInstance
	var upgradedCC *sysccprovider.ChaincodeInstance

	var vscc = &vsccValidatorImpl{
		support:     v.support,
		ccprovider:  ccprovider.GetChaincodeProvider(),
		sccprovider: sysccprovider.GetSystemChaincodeProvider()}

	sTime := time.Now()
	if payload, txResult = validation.ValidateTransaction(env); txResult != peer.TxValidationCode_VALID {
		txvalidator_log.WriteString(fmt.Sprintf("%s ValidateTransaction failed %d %+v\n", time.Now(), time.Now().Sub(sTime).Nanoseconds(), err))
		logger.Errorf("Invalid transaction with index %d", tIdx)
		return txResult, nil, nil, nil
	}
	txvalidator_log.WriteString(fmt.Sprintf("%s ValidateTransaction done %d\n", time.Now(), time.Now().Sub(sTime).Nanoseconds()))

	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		logger.Warningf("Could not unmarshal channel header, err %s, skipping", err)
		return peer.TxValidationCode_INVALID_OTHER_REASON, nil, nil, nil
	}

	channel := chdr.ChannelId
	logger.Debugf("Transaction is for chain %s", channel)

	if !v.chainExists(channel) {
		logger.Errorf("Dropping transaction for non-existent chain %s", channel)
		return peer.TxValidationCode_TARGET_CHAIN_NOT_FOUND, nil, nil, nil
	}

	if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {
		// Check duplicate transactions
		txID := chdr.TxId
		sTime := time.Now()
		if _, err := v.support.Ledger().GetTransactionByID(txID); err == nil {
			txvalidator_log.WriteString(fmt.Sprintf("%s GetTransactionById failed %d %+v\n", time.Now(), time.Now().Sub(sTime).Nanoseconds(), err))
			logger.Error("Duplicate transaction found, ", txID, ", skipping")
			return peer.TxValidationCode_DUPLICATE_TXID, nil, nil, nil
		}
		txvalidator_log.WriteString(fmt.Sprintf("%s GetTransactionById done %d\n", time.Now(), time.Now().Sub(sTime).Nanoseconds()))

		// Validate tx with vscc and policy
		logger.Debug("Validating transaction vscc tx validate")
		sTime = time.Now()
		err, cde := vscc.VSCCValidateTx(payload, d, env)
		//err = nil
		//cde := peer.TxValidationCode_VALID
		txvalidator_log.WriteString(fmt.Sprintf("%s VSCCValidateTx done %d %+v\n", time.Now(), time.Now().Sub(sTime).Nanoseconds(), err))
		if err != nil {
			txID := txID
			logger.Errorf("VSCCValidateTx for transaction txId = %s returned error %s", txID, err)
			return cde, nil, nil, nil
		}

		invokeCC, upgradeCC, err := v.getTxCCInstance(payload)
		if err != nil {
			logger.Errorf("Get chaincode instance from transaction txId = %s returned error %s", txID, err)
			return peer.TxValidationCode_INVALID_OTHER_REASON, nil, nil, nil
		}
		ccName = invokeCC
		if upgradeCC != nil {
			logger.Infof("Find chaincode upgrade transaction for chaincode %s on chain %s with new version %s", upgradeCC.ChaincodeName, upgradeCC.ChainID, upgradeCC.ChaincodeVersion)
			upgradedCC = upgradeCC
		}
	} else if common.HeaderType(chdr.Type) == common.HeaderType_CONFIG {
		configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
		if err != nil {
			err := fmt.Errorf("Error unmarshaling config which passed initial validity checks: %s", err)
			logger.Critical(err)
			// if we return an error, validation code doesn't matter
			return peer.TxValidationCode_INVALID_OTHER_REASON, nil, nil, err
		}

		if err := v.support.Apply(configEnvelope); err != nil {
			err := fmt.Errorf("Error validating config which passed initial validity checks: %s", err)
			logger.Critical(err)
			return peer.TxValidationCode_INVALID_OTHER_REASON, nil, nil, err
		}
		logger.Debugf("config transaction received for chain %s", channel)
	} else {
		logger.Warningf("Unknown transaction type [%s] in block number [%d] transaction index [%d]",
			common.HeaderType(chdr.Type), block.Header.Number, tIdx)
		return peer.TxValidationCode_UNKNOWN_TX_TYPE, nil, nil, nil
	}

	if _, err := proto.Marshal(env); err != nil {
		logger.Warningf("Cannot marshal transaction due to %s", err)
		return peer.TxValidationCode_MARSHAL_TX_ERROR, nil, nil, nil
	}
	// Succeeded to pass down here, transaction is valid
	return peer.TxValidationCode_VALID, ccName, upgradedCC, nil
}

func (v *txValidator) Validate(block *common.Block) error {
	logger.Debug("START Block Validation")
	defer logger.Debug("END Block Validation")

	startTime := time.Now()
	txvalidator_log.WriteString(fmt.Sprintf("%s Signature validation starts\n", startTime))
	defer txvalidator_log.WriteString(fmt.Sprintf("%s Signature validation ends %d\n", time.Now(), time.Now().Sub(startTime).Nanoseconds()))

	// don't reuse the cData cache for another block!
	defer func() {
		v.cDataMap.Lock()
		v.cDataMap.m = make(map[string]*ccprovider.ChaincodeData)
		v.cDataMap.Unlock()
	}()

	// Initialize trans as valid here, then set invalidation reason code upon invalidation below
	txsfltr := ledgerUtil.NewTxValidationFlags(len(block.Data.Data))
	// txsChaincodeNames records all the invoked chaincodes by tx in a block
	var txsChaincodeNamesMutex sync.Mutex
	txsChaincodeNames := make(map[int]*sysccprovider.ChaincodeInstance)
	// upgradedChaincodes records all the chaincodes that are upgrded in a block
	var txsUpgradedChaincodesMutex sync.Mutex
	txsUpgradedChaincodes := make(map[int]*sysccprovider.ChaincodeInstance)

	var stopMutex sync.RWMutex
	var stoppingError error

	var vsccWg sync.WaitGroup

	for tIdx, d := range block.Data.Data {
		// don't do any work if we don't have to.
		stopMutex.RLock()
		err := stoppingError
		stopMutex.RUnlock()
		if err != nil {
			return err
		}

		if d == nil {
			continue
		}

		env, err := utils.GetEnvelopeFromBlock(d)
		if err != nil {
			logger.Warningf("Error getting tx from block(%s)", err)
			// this is safe to call concurrently, unless the underlying object
			// gets changed from a slice to something else.
			txsfltr.SetFlag(tIdx, peer.TxValidationCode_INVALID_OTHER_REASON)
			continue
		} else if env == nil {
			logger.Warning("Nil tx from block")
			// this is safe to call concurrently, unless the underlying object
			// gets changed from a slice to something else.
			txsfltr.SetFlag(tIdx, peer.TxValidationCode_NIL_ENVELOPE)
			continue
		}

		<-vsccWorkerToken
		txvalidator_log.WriteString("Consumed token\n")
		vsccWg.Add(1)
		go func(tIdx int, d []byte) {
			txResult, txChName, txUpgradeCC, err := v.parallelVSCCValidateTx(block, tIdx, d, env)
			vsccWorkerToken <- struct{}{}
			txvalidator_log.WriteString("Token returned\n")
			vsccWg.Done()
			if err != nil {
				stopMutex.Lock()
				stoppingError = err
				stopMutex.Unlock()
				return
			}

			txsfltr.SetFlag(tIdx, txResult)
			if txChName != nil {
				txsChaincodeNamesMutex.Lock()
				txsChaincodeNames[tIdx] = txChName
				txsChaincodeNamesMutex.Unlock()
			}
			if txUpgradeCC != nil {
				txsUpgradedChaincodesMutex.Lock()
				txsUpgradedChaincodes[tIdx] = txUpgradeCC
				txsUpgradedChaincodesMutex.Unlock()
			}
		}(tIdx, d)
	}

	// wait for all vscc validations to finish
	vsccWg.Wait()
	txsfltr = v.invalidTXsForUpgradeCC(txsChaincodeNames, txsUpgradedChaincodes, txsfltr)

	// Initialize metadata structure
	utils.InitBlockMetadata(block)

	block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txsfltr

	return nil
}

// generateCCKey generates a unique identifier for chaincode in specific chain
func (v *txValidator) generateCCKey(ccName, chainID string) string {
	return fmt.Sprintf("%s/%s", ccName, chainID)
}

// invalidTXsForUpgradeCC invalid all txs that should be invalided because of chaincode upgrade txs
func (v *txValidator) invalidTXsForUpgradeCC(txsChaincodeNames map[int]*sysccprovider.ChaincodeInstance, txsUpgradedChaincodes map[int]*sysccprovider.ChaincodeInstance, txsfltr ledgerUtil.TxValidationFlags) ledgerUtil.TxValidationFlags {
	if len(txsUpgradedChaincodes) == 0 {
		return txsfltr
	}

	// Invalid former cc upgrade txs if there're two or more txs upgrade the same cc
	finalValidUpgradeTXs := make(map[string]int)
	upgradedChaincodes := make(map[string]*sysccprovider.ChaincodeInstance)
	for tIdx, cc := range txsUpgradedChaincodes {
		if cc == nil {
			continue
		}
		upgradedCCKey := v.generateCCKey(cc.ChaincodeName, cc.ChainID)

		if finalIdx, exist := finalValidUpgradeTXs[upgradedCCKey]; !exist {
			finalValidUpgradeTXs[upgradedCCKey] = tIdx
			upgradedChaincodes[upgradedCCKey] = cc
		} else if finalIdx < tIdx {
			logger.Infof("Invalid transaction with index %d: chaincode was upgraded by latter tx", finalIdx)
			txsfltr.SetFlag(finalIdx, peer.TxValidationCode_CHAINCODE_VERSION_CONFLICT)

			// record latter cc upgrade tx info
			finalValidUpgradeTXs[upgradedCCKey] = tIdx
			upgradedChaincodes[upgradedCCKey] = cc
		} else {
			logger.Infof("Invalid transaction with index %d: chaincode was upgraded by latter tx", tIdx)
			txsfltr.SetFlag(tIdx, peer.TxValidationCode_CHAINCODE_VERSION_CONFLICT)
		}
	}

	// invalid txs which invoke the upgraded chaincodes
	for tIdx, cc := range txsChaincodeNames {
		if cc == nil {
			continue
		}
		ccKey := v.generateCCKey(cc.ChaincodeName, cc.ChainID)
		if _, exist := upgradedChaincodes[ccKey]; exist {
			if txsfltr.IsValid(tIdx) {
				logger.Infof("Invalid transaction with index %d: chaincode was upgraded in the same block", tIdx)
				txsfltr.SetFlag(tIdx, peer.TxValidationCode_CHAINCODE_VERSION_CONFLICT)
			}
		}
	}

	return txsfltr
}

func (v *txValidator) getTxCCInstance(payload *common.Payload) (invokeCCIns, upgradeCCIns *sysccprovider.ChaincodeInstance, err error) {
	// This is duplicated unpacking work, but make test easier.
	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, nil, err
	}

	// Chain ID
	chainID := chdr.ChannelId // it is guaranteed to be an existing channel by now

	// ChaincodeID
	hdrExt, err := utils.GetChaincodeHeaderExtension(payload.Header)
	if err != nil {
		return nil, nil, err
	}
	invokeCC := hdrExt.ChaincodeId
	invokeIns := &sysccprovider.ChaincodeInstance{ChainID: chainID, ChaincodeName: invokeCC.Name, ChaincodeVersion: invokeCC.Version}

	// Transaction
	tx, err := utils.GetTransaction(payload.Data)
	if err != nil {
		logger.Errorf("GetTransaction failed: %s", err)
		return invokeIns, nil, nil
	}

	// ChaincodeActionPayload
	cap, err := utils.GetChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		logger.Errorf("GetChaincodeActionPayload failed: %s", err)
		return invokeIns, nil, nil
	}

	// ChaincodeProposalPayload
	cpp, err := utils.GetChaincodeProposalPayload(cap.ChaincodeProposalPayload)
	if err != nil {
		logger.Errorf("GetChaincodeProposalPayload failed: %s", err)
		return invokeIns, nil, nil
	}

	// ChaincodeInvocationSpec
	cis := &peer.ChaincodeInvocationSpec{}
	err = proto.Unmarshal(cpp.Input, cis)
	if err != nil {
		logger.Errorf("GetChaincodeInvokeSpec failed: %s", err)
		return invokeIns, nil, nil
	}

	if invokeCC.Name == "lscc" {
		if string(cis.ChaincodeSpec.Input.Args[0]) == "upgrade" {
			upgradeIns, err := v.getUpgradeTxInstance(chainID, cis.ChaincodeSpec.Input.Args[2])
			if err != nil {
				return invokeIns, nil, nil
			}
			return invokeIns, upgradeIns, nil
		}
	}

	return invokeIns, nil, nil
}

func (v *txValidator) getUpgradeTxInstance(chainID string, cdsBytes []byte) (*sysccprovider.ChaincodeInstance, error) {
	cds, err := utils.GetChaincodeDeploymentSpec(cdsBytes)
	if err != nil {
		return nil, err
	}

	return &sysccprovider.ChaincodeInstance{
		ChainID:          chainID,
		ChaincodeName:    cds.ChaincodeSpec.ChaincodeId.Name,
		ChaincodeVersion: cds.ChaincodeSpec.ChaincodeId.Version,
	}, nil
}

// GetInfoForValidate gets the ChaincodeInstance(with latest version) of tx, vscc and policy from lscc
func (v *vsccValidatorImpl) GetInfoForValidate(txid, chID, ccID string) (*sysccprovider.ChaincodeInstance, *sysccprovider.ChaincodeInstance, []byte, error) {
	cc := &sysccprovider.ChaincodeInstance{ChainID: chID}
	vscc := &sysccprovider.ChaincodeInstance{ChainID: chID}
	var policy []byte
	var err error
	if ccID != "lscc" {
		// when we are validating any chaincode other than
		// LSCC, we need to ask LSCC to give us the name
		// of VSCC and of the policy that should be used

		// obtain name of the VSCC and the policy from LSCC
		cd, err := v.getCDataForCC(ccID)
		if err != nil {
			logger.Errorf("Unable to get chaincode data from ledger for txid %s, due to %s", txid, err)
			return nil, nil, nil, err
		}
		cc.ChaincodeName = cd.Name
		cc.ChaincodeVersion = cd.Version
		vscc.ChaincodeName = cd.Vscc
		policy = cd.Policy
	} else {
		// when we are validating LSCC, we use the default
		// VSCC and a default policy that requires one signature
		// from any of the members of the channel
		cc.ChaincodeName = "lscc"
		cc.ChaincodeVersion = coreUtil.GetSysCCVersion()
		vscc.ChaincodeName = "vscc"
		p := cauthdsl.SignedByAnyMember(v.support.GetMSPIDs(chID))
		policy, err = utils.Marshal(p)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// Get vscc version
	vscc.ChaincodeVersion = coreUtil.GetSysCCVersion()

	return cc, vscc, policy, nil
}

func (v *vsccValidatorImpl) VSCCValidateTx(payload *common.Payload, envBytes []byte, env *common.Envelope) (error, peer.TxValidationCode) {
	// get header extensions so we have the chaincode ID
	hdrExt, err := utils.GetChaincodeHeaderExtension(payload.Header)
	if err != nil {
		return err, peer.TxValidationCode_BAD_HEADER_EXTENSION
	}

	// get channel header
	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return err, peer.TxValidationCode_BAD_CHANNEL_HEADER
	}

	/* obtain the list of namespaces we're writing stuff to;
	   at first, we establish a few facts about this invocation:
	   1) which namespaces does it write to?
	   2) does it write to LSCC's namespace?
	   3) does it write to any cc that cannot be invoked? */
	wrNamespace := []string{}
	writesToLSCC := false
	writesToNonInvokableSCC := false
	respPayload, err := utils.GetActionFromEnvelope(envBytes)
	if err != nil {
		return fmt.Errorf("GetActionFromEnvelope failed, error %s", err), peer.TxValidationCode_BAD_RESPONSE_PAYLOAD
	}
	txRWSet := &rwsetutil.TxRwSet{}
	if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
		return fmt.Errorf("txRWSet.FromProtoBytes failed, error %s", err), peer.TxValidationCode_BAD_RWSET
	}
	for _, ns := range txRWSet.NsRwSets {
		if len(ns.KvRwSet.Writes) > 0 {
			wrNamespace = append(wrNamespace, ns.NameSpace)

			if !writesToLSCC && ns.NameSpace == "lscc" {
				writesToLSCC = true
			}

			if !writesToNonInvokableSCC && v.sccprovider.IsSysCCAndNotInvokableCC2CC(ns.NameSpace) {
				writesToNonInvokableSCC = true
			}

			if !writesToNonInvokableSCC && v.sccprovider.IsSysCCAndNotInvokableExternal(ns.NameSpace) {
				writesToNonInvokableSCC = true
			}
		}
	}

	// get name and version of the cc we invoked
	ccID := hdrExt.ChaincodeId.Name
	ccVer := respPayload.ChaincodeId.Version

	// sanity check on ccID
	if ccID == "" {
		err := fmt.Errorf("invalid chaincode ID")
		logger.Errorf("%s", err)
		return err, peer.TxValidationCode_INVALID_OTHER_REASON
	}
	if ccID != respPayload.ChaincodeId.Name {
		err := fmt.Errorf("inconsistent ccid info (%s/%s)", ccID, respPayload.ChaincodeId.Name)
		logger.Errorf("%s", err)
		return err, peer.TxValidationCode_INVALID_OTHER_REASON
	}
	// sanity check on ccver
	if ccVer == "" {
		err := fmt.Errorf("invalid chaincode version")
		logger.Errorf("%s", err)
		return err, peer.TxValidationCode_INVALID_OTHER_REASON
	}

	// we've gathered all the info required to proceed to validation;
	// validation will behave differently depending on the type of
	// chaincode (system vs. application)

	if !v.sccprovider.IsSysCC(ccID) {
		// if we're here, we know this is an invocation of an application chaincode;
		// first of all, we make sure that:
		// 1) we don't write to LSCC - an application chaincode is free to invoke LSCC
		//    for instance to get information about itself or another chaincode; however
		//    these legitimate invocations only ready from LSCC's namespace; currently
		//    only two functions of LSCC write to its namespace: deploy and upgrade and
		//    neither should be used by an application chaincode
		if writesToLSCC {
			return fmt.Errorf("Chaincode %s attempted to write to the namespace of LSCC", ccID),
				peer.TxValidationCode_ILLEGAL_WRITESET
		}
		// 2) we don't write to the namespace of a chaincode that we cannot invoke - if
		//    the chaincode cannot be invoked in the first place, there's no legitimate
		//    way in which a transaction has a write set that writes to it; additionally
		//    we don't have any means of verifying whether the transaction had the rights
		//    to perform that write operation because in v1, system chaincodes do not have
		//    any endorsement policies to speak of. So if the chaincode can't be invoked
		//    it can't be written to by an invocation of an application chaincode
		if writesToNonInvokableSCC {
			return fmt.Errorf("Chaincode %s attempted to write to the namespace of a system chaincode that cannot be invoked", ccID),
				peer.TxValidationCode_ILLEGAL_WRITESET
		}

		// validate *EACH* read write set according to its chaincode's endorsement policy
		for _, ns := range wrNamespace {
			// Get latest chaincode version, vscc and validate policy
			txcc, vscc, policy, err := v.GetInfoForValidate(chdr.TxId, chdr.ChannelId, ns)
			if err != nil {
				logger.Errorf("GetInfoForValidate for txId = %s returned error %s", chdr.TxId, err)
				return err, peer.TxValidationCode_INVALID_OTHER_REASON
			}

			// if the namespace corresponds to the cc that was originally
			// invoked, we check that the version of the cc that was
			// invoked corresponds to the version that lscc has returned
			if ns == ccID && txcc.ChaincodeVersion != ccVer {
				err := fmt.Errorf("Chaincode %s:%s/%s didn't match %s:%s/%s in lscc", ccID, ccVer, chdr.ChannelId, txcc.ChaincodeName, txcc.ChaincodeVersion, chdr.ChannelId)
				logger.Errorf(err.Error())
				return err, peer.TxValidationCode_EXPIRED_CHAINCODE
			}

			// do VSCC validation
			if err = v.VSCCValidateTxForCC(envBytes, chdr.TxId, chdr.ChannelId, vscc.ChaincodeName, vscc.ChaincodeVersion, policy); err != nil {
				return fmt.Errorf("VSCCValidateTxForCC failed for cc %s, error %s", ccID, err),
					peer.TxValidationCode_ENDORSEMENT_POLICY_FAILURE
			}
		}
	} else {
		// make sure that we can invoke this system chaincode - if the chaincode
		// cannot be invoked through a proposal to this peer, we have to drop the
		// transaction; if we didn't, we wouldn't know how to decide whether it's
		// valid or not because in v1, system chaincodes have no endorsement policy
		if v.sccprovider.IsSysCCAndNotInvokableExternal(ccID) {
			return fmt.Errorf("Committing an invocation of cc %s is illegal", ccID),
				peer.TxValidationCode_ILLEGAL_WRITESET
		}

		// Get latest chaincode version, vscc and validate policy
		_, vscc, policy, err := v.GetInfoForValidate(chdr.TxId, chdr.ChannelId, ccID)
		if err != nil {
			logger.Errorf("GetInfoForValidate for txId = %s returned error %s", chdr.TxId, err)
			return err, peer.TxValidationCode_INVALID_OTHER_REASON
		}

		// validate the transaction as an invocation of this system chaincode;
		// vscc will have to do custom validation for this system chaincode
		// currently, VSCC does custom validation for LSCC only; if an hlf
		// user creates a new system chaincode which is invokable from the outside
		// they have to modify VSCC to provide appropriate validation
		if err = v.VSCCValidateTxForCC(envBytes, chdr.TxId, vscc.ChainID, vscc.ChaincodeName, vscc.ChaincodeVersion, policy); err != nil {
			return fmt.Errorf("VSCCValidateTxForCC failed for cc %s, error %s", ccID, err),
				peer.TxValidationCode_ENDORSEMENT_POLICY_FAILURE
		}
	}

	return nil, peer.TxValidationCode_VALID
}

func (v *vsccValidatorImpl) VSCCValidateTxForCC(envBytes []byte, txid, chid, vsccName, vsccVer string, policy []byte) error {
	ctxt, err := v.ccprovider.GetContext(v.support.Ledger())
	if err != nil {
		logger.Errorf("Cannot obtain context for txid=%s, err %s", txid, err)
		return err
	}
	defer v.ccprovider.ReleaseContext()

	// build arguments for VSCC invocation
	// args[0] - function name (not used now)
	// args[1] - serialized Envelope
	// args[2] - serialized policy
	args := [][]byte{[]byte(""), envBytes, policy}

	// get context to invoke VSCC
	vscctxid := coreUtil.GenerateUUID()
	cccid := v.ccprovider.GetCCContext(chid, vsccName, vsccVer, vscctxid, true, nil, nil)

	// invoke VSCC
	logger.Debug("Invoking VSCC txid", txid, "chaindID", chid)
	startTime := time.Now()
	res, _, err := v.ccprovider.ExecuteChaincode(ctxt, cccid, args)
	txvalidator_log.WriteString(fmt.Sprintf("%s RealVSCC done %d\n", time.Now(), time.Now().Sub(startTime).Nanoseconds()))
	if err != nil {
		logger.Errorf("Invoke VSCC failed for transaction txid=%s, error %s", txid, err)
		return err
	}
	if res.Status != shim.OK {
		logger.Errorf("VSCC check failed for transaction txid=%s, error %s", txid, res.Message)
		return fmt.Errorf("%s", res.Message)
	}

	return nil
}

func (v *vsccValidatorImpl) getCDataForCC(ccid string) (*ccprovider.ChaincodeData, error) {
	v.cDataMap.RLock()
	cdata, ok := v.cDataMap.m[ccid]
	v.cDataMap.RUnlock()

	if ok {
		return cdata, nil
	}

	l := v.support.Ledger()
	if l == nil {
		return nil, fmt.Errorf("nil ledger instance")
	}

	qe, err := l.NewQueryExecutor()
	if err != nil {
		return nil, fmt.Errorf("Could not retrieve QueryExecutor, error %s", err)
	}
	defer qe.Done()

	bytes, err := qe.GetState("lscc", ccid)
	if err != nil {
		return nil, fmt.Errorf("Could not retrieve state for chaincode %s, error %s", ccid, err)
	}

	if bytes == nil {
		return nil, fmt.Errorf("lscc's state for [%s] not found.", ccid)
	}

	cd := &ccprovider.ChaincodeData{}
	err = proto.Unmarshal(bytes, cd)
	if err != nil {
		return nil, fmt.Errorf("Unmarshalling ChaincodeQueryResponse failed, error %s", err)
	}

	if cd.Vscc == "" {
		return nil, fmt.Errorf("lscc's state for [%s] is invalid, vscc field must be set.", ccid)
	}

	if len(cd.Policy) == 0 {
		return nil, fmt.Errorf("lscc's state for [%s] is invalid, policy field must be set.", ccid)
	}

	v.cDataMap.Lock()
	v.cDataMap.m[ccid] = cd
	v.cDataMap.Unlock()

	return cd, err
}
