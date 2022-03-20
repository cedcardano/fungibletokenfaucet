from cardano.wallet import Wallet
from cardano.wallet import WalletService
from cardano.backends.walletrest import WalletREST
from cardano.simpletypes import AssetID
from cardano.exceptions import CannotCoverFee
from cardano.backends.walletrest.exceptions import RESTServerError
from blockfrost import BlockFrostApi, ApiError, ApiUrls
from decimal import *
import time
from datetime import datetime
from datetime import timedelta
import random
import json
import os
import requests
import functools
from datetime import timezone
# TODO CREATE A SUPERCLASS FOR FAUCET AND SWAP
# Make print statements occur at the top level not the bottom
# make the write to log methods return instead of print directly
# make runloop able to access all the variables - maybe as in instance variable only used for logging
# so reliability isn't strict

# the faucet requires at least the very least 3*bundlesize ADA to function (more if your tokens are bundled with more than ~1.5ADA for each output),
# but throughput is greatly increased when there is more spare ADA in the wallet
# due to increased fragmentation and therefore there are lots of UTXOs to select from.
# throughput may be slow at first if your ADA is in a big chunk,
# but should increase as UTXOs are increasingly fragmented and the risk of contention is reduced.
# you can fragment the ADA yourself by breaking it into smaller UTXO chunks, but every distribution of 25 token UTXOs
# comes with 25 change UTXOs, so fragmentation should happen by itself fairly quickly.
# you can reduce bundlesize to reduce the minimum amount of ADA needed to operate, but this increases the number of transactions needed
# and thus fees. Do NOT increase bundlesize past 25 - higher amounts risk transactions failing due to exceeding size limitations.

# for maximum throughput I would recommend having at least 500ADA in the wallet, or even 2000+ if you want to
# loop 5 times a minute and approach Blockfrost's bottleneck of 500 tx per minute

host_port = "http://192.168.20.12:3000/"


def send_req(url_payload: str) -> requests.Response:
    return requests.get(host_port + url_payload)


def fromHex(hexStr: str) -> str:
    return bytearray.fromhex(hexStr).decode()


def toHex(utfStr: str) -> str:
    return utfStr.encode("utf-8").hex()


def remove_slash_x(raw_hex_str: str) -> str:
    return raw_hex_str[2:]


def get_tx_dbid_list(txid_list: list[str]) -> list[dict]:
    return send_req("tx?hash=in.({})".format(functools.reduce(lambda a, b: a+','+b, ['\\x'+txid for txid in txid_list]))).json()


def get_tx_out_dbid_list(txdbid_list: list[int]) -> list[dict]:
    return send_req(
        f"tx_out?tx_id=in.({functools.reduce(lambda a, b: f'{str(a)},{str(b)}', txdbid_list)})"
    ).json()


def get_ma_tx_out_dbid_list(tx_out_dbid_list: list[int]) -> list[dict]:
    return send_req(
        f"ma_tx_out?tx_out_id=in.({functools.reduce(lambda a, b: str(a)+','+str(b), tx_out_dbid_list)})"
    ).json()


def get_multi_asset_dbid_list(list_idents: int) -> list[dict]:
    if list_idents:
        return send_req(
            f"multi_asset?id=in.({functools.reduce(lambda a, b: f'{a},{b}', [str(ident) for ident in list_idents])})&select=id,policy,name"
        ).json()

    else:
        return []


def get_metadata_dbid_list(txdbid_list: list[int]) -> list[dict]:
    return send_req(
        f"tx_metadata?tx_id=in.({functools.reduce(lambda a, b: f'{str(a)},{str(b)}', txdbid_list)})"
    ).json()


def get_tx_in_dbid_list(txdbid_list: list[int]) -> list[dict]:
    return send_req(
        f"tx_in?tx_in_id=in.({functools.reduce(lambda a, b: f'{str(a)},{str(b)}', txdbid_list)})"
    ).json()


def get_tx_by_dbid_list(txdbid_list: list[int]) -> list[dict]:
    return send_req(
        f"tx?id=in.({functools.reduce(lambda a, b: f'{str(a)},{str(b)}', txdbid_list)})"
    ).json()


class Faucet:

    # constructor
    # apiKey: str - Blockfrost Api Key
    # assetName: str - hex name of asset
    # assetPolicyID: str - policy id of asset
    # walletID: str - reference ID of wallet used by cardano-wallet
    # faucetAddr: str - receiving address of faucet
    # port: int - port that cardano-wallet is broadcasting on

    def __init__(self, apiKey, assetName, assetPolicyID, walletID, faucetAddr, pullcost=2000000, pullprofit=500000, proportionperpull=0.000015, port=8090, host="localhost", discord=False):
        self.assetName = assetName
        self.assetPolicyID = assetPolicyID
        self.assetIDObj = AssetID(assetName, assetPolicyID)
        self.wallet = Wallet(
            walletID, backend=WalletREST(port=port, host=host))
        self.faucetAddr = faucetAddr
        self.bundlesize = None

        self.logFile = assetName+assetPolicyID+faucetAddr+"v2.json"

        self.pullcost = Decimal(str(pullcost/1000000))
        self.proportionperpull = proportionperpull
        self.pullprofit = Decimal(str(pullprofit/1000000))

        # serve on port localhost port 5001
        self.discord = discord
        self.logging = True

        self.printiflog("Faucet Created.\n")

        self.pending_discord_topup = []

    def runloop(self, passphrase, period=300, loops=10000, bundlesize=20, multsallowed=1):
        self.bundlesize = bundlesize

        for _ in range(loops):
            timenow = datetime.now()
            self.printiflog(f"SYS TIME:    {str(timenow)[:-7]}")

            self.sendtokens(passphrase, multsallowed=multsallowed)
            self.printiflog("___________________FAUCET___________________")
            time.sleep(period)

    def sendtokens(self, passphrase, multsallowed: int = 1):
        if multsallowed < 1 or (not isinstance(multsallowed, int)):
            raise ValueError("Illegal multsallowed parameter.")
        try:
            remainingtokens = self.readAssetBalance()
            currpullscount = self.readPullsCount()
        except FileNotFoundError as e:
            raise FileNotFoundError(
                "You have not generated the blockchain index files. Please call generateLog."
            ) from e

        incomingtxs = self.get_new_incoming_txs()

        assetFilteredTxs = self.filtered_incomings_discard_assets(incomingtxs)
        senderaddrdict = self.get_sender_addr_dict(
            [tx.txid for tx in assetFilteredTxs])

        sendlist, numpulls = self.prepare_sendlist(
            assetFilteredTxs, senderaddrdict, multsallowed, remainingtokens)
        if self.discord:
            if prepare_topups := self.prepare_discord_topups():
                sendlist += prepare_topups
                discord_topup = True
            else:
                discord_topup = False

        if len(sendlist) > 0:
            total_send, outbound_txs = self.autoSendAssets(
                sendlist, passphrase)
            if discord_topup:
                self.pending_discord_topup = [tx.txid for tx in outbound_txs]
            self.printiflog(f"TOKENS SENT: {str(total_send)}")

            self.printiflog(f"No. Pulls:   {str(numpulls)}")
            self.writePullsCount(numpulls+currpullscount)

    def autoSendAssets(self, pendingTxList, passphrase):
        remainingtokens = self.readAssetBalance()
        total_send = sum(pendingtx['pullyield'] for pendingtx in pendingTxList)

        if self.bundlesize is None:
            self.bundlesize = 25
        groupsof = []
        smallarray = []

        # break into groups of bundlesize (25 by default) due to tx size constraints
        for i in range(len(pendingTxList)):
            if i == len(pendingTxList)-1:
                smallarray.append(pendingTxList[i])
                groupsof.append(smallarray)
            else:
                if len(smallarray) == self.bundlesize:
                    groupsof.append(smallarray)
                    smallarray = []
                smallarray.append(pendingTxList[i])

        for groupof in groupsof:
            destinations = []
            for pendingtx in groupof:
                #{"senderaddr": senderaddr, "pullyield": randomyield, "returnada": returnada}
                if pendingtx['pullyield'] != 0:
                    destinations.append((pendingtx['senderaddr'], pendingtx['returnada'], [
                                        (self.assetIDObj, pendingtx['pullyield'])]))
                else:
                    destinations.append(
                        (pendingtx['senderaddr'], pendingtx['returnada']))

            attempts = 0
            sent = False
            outbound_txs = []
            while not sent:
                # loop 10 times, 30 seconds each, to give time for any pending transactions to be broadcast and free up UTXOs to build the next transaction
                # if it still doesn't go through after 300 seconds of pause, the wallet has probably run out of funds, or the blockchain is
                # ridiculously congested
                try:
                    outboundtx = self.wallet.transfer_multiple(
                        destinations, passphrase=passphrase)
                    outbound_txs.append(outboundtx)
                    sent = True
                except CannotCoverFee as e:
                    if attempts == 11:
                        raise CannotCoverFee(
                            "There is likely insufficient funds in your wallet to distribute the requested tokens."
                        ) from e

                    attempts += 1
                    time.sleep(30)
                except RESTServerError as e:
                    if attempts == 11:
                        raise RESTServerError(
                            "There are likely insufficient tokens in your wallet to distribute."
                        ) from e

                    attempts += 1
                    time.sleep(30)

        self.writeAssetBalance(remainingtokens - total_send)
        return total_send, outbound_txs

    def prepare_discord_topups(self) -> list[dict[str, str | int | Decimal]]:
        appendPendingTxList = []
        if not self.pending_discord_topup:
            sessionsDict = requests.get(
                "http://127.0.0.1:5001/sessions").json()
            for addr, topupAmount in sessionsDict.items():
                elemDict = {'senderaddr': addr, 'returnada': Decimal(
                    "1.5"), 'pullyield': topupAmount}
                appendPendingTxList.append(elemDict)
        return appendPendingTxList

    def prepare_sendlist(self, filtered_txs, sender_addr_dict, multsallowed, remainingtokens):
        sendlist = []
        numpulls = 0

        for tx in filtered_txs:
            try:
                senderaddr = sender_addr_dict[tx.txid]
            except KeyError:
                senderaddr = self.get_sender_addr_dict([tx.txid])[tx.txid]

            txoutputs = list(tx.local_outputs)

            countedoutput = txoutputs[0]
            extraoutputs = txoutputs[1:]

            if countedoutput.amount >= self.pullcost:
                validmults = int(
                    min(multsallowed, countedoutput.amount // self.pullcost))
                returnada = countedoutput.amount - validmults*self.pullprofit

                randomyield = 0
                for _ in range(validmults):
                    onetrial = self.calculateYield(
                        self.proportionperpull, remainingtokens)
                    remainingtokens -= onetrial
                    randomyield += onetrial

                sendlist.append(
                    {"senderaddr": senderaddr, "pullyield": randomyield, "returnada": returnada})

                numpulls += validmults

            sendlist.extend(
                {
                    "senderaddr": senderaddr,
                    "pullyield": 0,
                    "returnada": output.amount,
                }
                for output in extraoutputs
            )

        return sendlist, numpulls

    def get_new_txs(self):
        try:
            lasttime = self.readLastTime()
            lastslot = self.readSlot()
        except FileNotFoundError:
            lastslot = 1
            lasttime = datetime.now(timezone.utc)

        currenttime = datetime.now(timezone.utc)
        self.printiflog(
            f"TIME INT:    {str(lasttime)[:-7]} to {str(currenttime)[:-7]}")
        newtxs = self.wallet.txsfiltered(lasttime)
        in_ledger_txs = [tx for tx in newtxs if tx.status == "in_ledger"]
        return in_ledger_txs, lastslot

    def get_new_incoming_txs(self):
        newtxs, lastslot = self.get_new_txs()

        if len(newtxs) == 0:
            return []

        incomingtxs = []
        for tx in newtxs:
            # local_inputs == [] means incoming transaction - these are necessarily confirmed already
            if tx.local_inputs == []:
                if tx.inserted_at.absolute_slot > lastslot:
                    incomingtxs.append(tx)
            elif tx.txid in self.pending_discord_topup:
                self.pending_discord_topup.remove(tx.txid)
        if incomingtxs:
            newlastslot = newtxs[-1].inserted_at.absolute_slot
            newlasttime = self.isostringtodt(newtxs[-1].inserted_at.time)
            self.writeLastTime(newlasttime)
            self.writeSlot(newlastslot)

        return incomingtxs

    def filtered_incomings_discard_assets(self, incomingtxs):
        return [
            tx
            for tx in incomingtxs
            if not bool(
                sum(output.assets != [] for output in list(tx.local_outputs))
            )
        ]

    def get_sender_addr_dict(self, txid_list: list[str]) -> dict[str, str]:

        return_list = self.db_sync_tx_utxos(txid_list)
        return {txdict['tx_hash']: txdict['inputs'][0]['payment_addr']['bech32'] for txdict in return_list}

    def db_sync_tx_utxos(self, txhashlist):
        return self.get_postgrest_req_caller(txhashlist)

    def get_postgrest_req_caller(self, list_txids: list[str]):

        chunks_of_1k, remainder = divmod(len(list_txids), 100)
        request_chunk_array = [
            list_txids[100 * i: 100 * (i + 1)] for i in range(chunks_of_1k)
        ]

        request_chunk_array.append(list_txids[100*chunks_of_1k:])

        txs_list_json = []

        for chunkcount, chunk in enumerate(request_chunk_array, start=1):
            while True:
                try:
                    txs_list_json_local = self.get_postgrest_req(chunk)
                    break
                except Exception as e:
                    if self.logging:
                        print(e)
                        print(
                            "Postgrest chunk request failed. Requerying in 5 seconds...")
                    time.sleep(5)
            txs_list_json += txs_list_json_local
        return txs_list_json

    def get_postgrest_req(self, list_txids: list[str]):
        if not list_txids:
            return []
        tx_list = get_tx_dbid_list(list_txids)
        tx_out_list = get_tx_out_dbid_list(
            (txdbids := [tx['id'] for tx in tx_list]))
        metadata_list = get_metadata_dbid_list(txdbids)
        tx_ma_out_list = get_ma_tx_out_dbid_list(
            [tx_out['id'] for tx_out in tx_out_list])
        multi_asset_list = get_multi_asset_dbid_list(
            [ma_out['ident'] for ma_out in tx_ma_out_list])

        tx_out_list_by_txdbid = {tx['id']: [] for tx in tx_list}
        for tx_out in tx_out_list:
            tx_out_list_by_txdbid[tx_out['tx_id']].append(tx_out)

        metadata_list_by_txdbid = {tx['id']: [] for tx in tx_list}
        for metadata in metadata_list:
            metadata_list_by_txdbid[metadata['tx_id']].append(metadata)

        tx_ma_out_list_by_tx_out_dbid = {
            tx_out['id']: [] for tx_out in tx_out_list}
        for tx_ma_out in tx_ma_out_list:
            tx_ma_out_list_by_tx_out_dbid[tx_ma_out['tx_out_id']].append(
                tx_ma_out)

        multi_asset_list_by_dbid = {ma['id']: ma for ma in multi_asset_list}

        # inputs
        tx_in_list = get_tx_in_dbid_list(txdbids)
        tx_in_list_by_txdbid = {tx['id']: [] for tx in tx_list}
        for tx_in in tx_in_list:
            tx_in_list_by_txdbid[tx_in['tx_in_id']].append(tx_in)

        tx_in_output_keys = [(txin['tx_out_id'], txin['tx_out_index'])
                             for txin in tx_in_list]
        tx_in_out_list = list(filter(lambda txout: (txout['tx_id'], txout['index']) in tx_in_output_keys, get_tx_out_dbid_list(
            txin['tx_out_id'] for txin in tx_in_list)))
        tx_in_out_dict = {(txout['tx_id'], txout['index'])                          : txout for txout in tx_in_out_list}

        tx_in_tx_list = get_tx_by_dbid_list(
            [tx['tx_id'] for tx in tx_in_out_list])
        tx_in_tx_to_tx_hash = {tx['id']: remove_slash_x(
            tx['hash']) for tx in tx_in_tx_list}

        # recurse once to get input info

        returnlist = []
        for tx in tx_list:
            tx_returndict = {'tx_hash': remove_slash_x(
                tx['hash']), 'block_height': tx['block_id'], 'tx_block_index': tx['block_index'], 'fee': tx['fee']}

            outputs = tx_out_list_by_txdbid[tx['id']]
            outputs_list = []
            for output in outputs:
                output_dict = {"payment_addr": {"bech32": output['address'], "cred": remove_slash_x(
                    output["payment_cred"])}, "value": output['value'], "tx_index": output['index']}

                tx_ma_outs = tx_ma_out_list_by_tx_out_dbid[output['id']]
                assets_list = []
                for tx_ma_out in tx_ma_outs:
                    asset_dict = {'quantity': tx_ma_out['quantity']}
                    multi_asset = multi_asset_list_by_dbid[tx_ma_out['ident']]
                    asset_dict['policy_id'] = remove_slash_x(
                        multi_asset['policy'])
                    asset_dict['asset_name'] = remove_slash_x(
                        multi_asset['name'])
                    assets_list.append(asset_dict)
                output_dict['asset_list'] = assets_list

                outputs_list.append(output_dict)

            tx_returndict['outputs'] = outputs_list

            metadata_list = []
            for entry in metadata_list_by_txdbid[tx['id']]:
                key = int(entry['key'])
                json = entry['json']
                metadata_list.append({'key': key, 'json': json})
            tx_returndict['metadata'] = metadata_list

            # inputs
            inputs = tx_in_list_by_txdbid[tx['id']]
            inputs_list = []
            for input in inputs:
                key_tuple = (input['tx_out_id'], input['tx_out_index'])
                corresponding_output = tx_in_out_dict[key_tuple]

                input_dict = {
                    'payment_addr': {
                        'bech32': corresponding_output['address'],
                        'cred': remove_slash_x(
                            corresponding_output["payment_cred"]
                        ),
                    }
                }

                input_dict['value'] = corresponding_output['value']
                input_dict['tx_hash'] = tx_in_tx_to_tx_hash[corresponding_output['tx_id']]
                input_dict['tx_index'] = input['tx_out_index']

                inputs_list.append(input_dict)

            tx_returndict['inputs'] = inputs_list

            returnlist.append(tx_returndict)

        return returnlist

    def printiflog(self, printstring):
        if self.logging:
            print(printstring)

#############################################################
    @staticmethod
    def dttodict(dt):
        return {"year": dt.year, "month": dt.month, "day": dt.day, "hour": dt.hour, "minute": dt.minute, "second": dt.second,  "mus": dt.microsecond}

    @staticmethod
    def dicttodt(dtdict):
        return datetime(dtdict['year'], dtdict['month'], dtdict['day'], dtdict['hour'], dtdict['minute'], dtdict['second'], dtdict['mus'])

    @staticmethod
    def isostringtodt(isostring):
        return datetime(
            int(isostring[:4]),
            int(isostring[5:7]),
            int(isostring[8:10]),
            int(isostring[11:13]),
            int(isostring[14:16]),
            int(isostring[17:19]),
            1,
        )

    def generateLog(self, initTokenbalance, totalpulls):
        timenow = datetime.now(timezone.utc)
        logdict = {"tokenBalance": [initTokenbalance], "txTime": [
            self.dttodict(timenow)], "totalPulls": [totalpulls], "slot": [1]}

        with open(self.logFile, 'w') as f:
            json.dump(logdict, f)

    def readLog(self):
        with open(self.logFile, 'r') as f:
            return json.load(f)

    def writeLog(self, logDict):
        with open(self.logFile, 'w') as f:
            json.dump(logDict, f)

    def rollback(self):
        logDict = self.readLog()
        logDict['txTime'].append(logDict['txTime'][-2])
        logDict['slot'].append(logDict['slot'][-2])
        self.writeLog(logDict)

    def readLastTime(self):
        logDict = self.readLog()
        return self.dicttodt(logDict['txTime'][-1])

    def writeLastTime(self, dt):
        logDict = self.readLog()
        logDict['txTime'].append(self.dttodict(dt))
        self.writeLog(logDict)

    def readSlot(self):
        logDict = self.readLog()
        return logDict['slot'][-1]

    def writeSlot(self, slot):
        logDict = self.readLog()
        logDict['slot'].append(slot)
        self.writeLog(logDict)

    def readAssetBalance(self):
        logDict = self.readLog()
        return logDict['tokenBalance'][-1]

    def writeAssetBalance(self, balance):
        logDict = self.readLog()
        logDict['tokenBalance'].append(balance)
        self.writeLog(logDict)

        print(f"TOKENS REM:  {str(balance)}\n")

    def readPullsCount(self):
        logDict = self.readLog()
        return logDict['totalPulls'][-1]

    def writePullsCount(self, balance):
        logDict = self.readLog()
        logDict['totalPulls'].append(balance)
        self.writeLog(logDict)

        print(f"Tot. Pulls:  {str(balance)}")

###############################################################
    # save processed transactions to file

    @staticmethod
    def calculateYield(proportionperpull, remainingtokens):
        return int(round(2*random.betavariate(12, 12)*int(round(remainingtokens*proportionperpull))))

    @staticmethod
    def hexencode(utf8str):
        return utf8str.encode("utf-8").hex()

    @staticmethod
    def hexdecode(hexstr):
        return bytes.fromhex(hexstr).decode("utf-8")


class Swapper:

    # constructor
    # apiKey: str - Blockfrost Api Key
    # assetName: str - hex name of asset
    # assetPolicyID: str - policy id of asset
    # walletID: str - reference ID of wallet used by cardano-wallet
    # faucetAddr: str - receiving address of faucet
    # port: int - port that cardano-wallet is broadcasting on

    def __init__(self, apiKey, receiveAssetName, receiveAssetPolicyID, sendAssetName, sendAssetPolicyID, walletID, swapperAddr, port=8090, host="localhost"):
        self.api = BlockFrostApi(project_id=apiKey)
        self.wallet = Wallet(
            walletID, backend=WalletREST(port=port, host=host))

        self.receiveAssetIDObj = AssetID(
            receiveAssetName, receiveAssetPolicyID)
        self.sendAssetIDObj = AssetID(sendAssetName, sendAssetPolicyID)

        self.swapperAddr = swapperAddr
        self.bundlesize = None

        self.logFile = receiveAssetName+receiveAssetPolicyID + \
            sendAssetName+sendAssetPolicyID+swapperAddr+"swap.json"

        print("Token swap service tool created.\n")

    def runloop(self, passphrase, period=300, loops=10000, bundlesize=20):
        self.bundlesize = bundlesize

        for _ in range(loops):
            timenow = datetime.now()
            print(f"SYS TIME:    {str(timenow)[:-7]}")

            self.swaptokens(passphrase)
            print("____________________SWAP____________________")
            time.sleep(period)

    def swaptokens(self, passphrase):

        try:
            remainingtokens = self.readAssetBalance()
            lasttime = self.readLastTime()
            lastslot = self.readSlot()

        except FileNotFoundError as e:
            raise FileNotFoundError(
                "You have not generated the blockchain index files. Please call generateLog."
            ) from e
############################################################################################################

        currenttime = datetime.now(timezone.utc)
        print(f"TIME INT:    {str(lasttime)[:-7]} to {str(currenttime)[:-7]}")
        newtxs = self.wallet.txsfiltered(lasttime)

        incomingtxs = [
            tx
            for tx in newtxs
            if tx.local_inputs == [] and tx.inserted_at.absolute_slot > lastslot
        ]

        if incomingtxs:
            newlastslot = incomingtxs[-1].inserted_at.absolute_slot
            newlasttime = self.isostringtodt(incomingtxs[-1].inserted_at.time)
            self.writeLastTime(newlasttime)
            self.writeSlot(newlastslot)

        sendlist = []
        tokensswapped = 0

        for tx in incomingtxs:
            txoutputs = list(tx.local_outputs)

            totalcorrecttokens = 0
            totalada = Decimal(0)

            containsAssets = False
            for output in txoutputs:
                totalada += output.amount
                for assettuple in output.assets:
                    if assettuple[0] == self.receiveAssetIDObj:
                        containsAssets = True
                        totalcorrecttokens += assettuple[1]

            if containsAssets:
                senderaddr = None
                attempt = 0
                while senderaddr is None:
                    try:
                        senderaddr = self.api.transaction_utxos(
                            hash=tx.txid).inputs[0].address
                    except ApiError as e:
                        attempt += 1
                        print(
                            f"Sender address fetch attempt {attempt} API Error {str(e.status_code)} - reattempting.")
                        time.sleep(3)

                    sendlist.append(
                        {"senderaddr": senderaddr, "tokenpayload": totalcorrecttokens, "returnada": totalada})

                    tokensswapped += totalcorrecttokens

        if sendlist:
            self.autoSendAssets(sendlist, passphrase)
            print(f"TKN SWAPPED: {str(tokensswapped)}")
            self.writeAssetBalance(remainingtokens-tokensswapped)


############################################################################


    def autoSendAssets(self, pendingTxList, passphrase):
        if self.bundlesize is None:
            self.bundlesize = 25
        groupsof = []
        smallarray = []

        # break into groups of bundlesize (25 by default) due to tx size constraints
        for i in range(len(pendingTxList)):
            if i == len(pendingTxList)-1:
                smallarray.append(pendingTxList[i])
                groupsof.append(smallarray)
            else:
                if len(smallarray) == self.bundlesize:
                    groupsof.append(smallarray)
                    smallarray = []
                smallarray.append(pendingTxList[i])

        for groupof in groupsof:
            destinations = []
            for pendingtx in groupof:
                #{"senderaddr": senderaddr, "tokenpayload": randomyield, "returnada": returnada}
                if pendingtx['tokenpayload'] != 0:
                    destinations.append((pendingtx['senderaddr'], pendingtx['returnada'], [
                                        (self.sendAssetIDObj, pendingtx['tokenpayload'])]))
                else:
                    destinations.append(
                        (pendingtx['senderaddr'], pendingtx['returnada']))

            attempts = 0
            sent = False
            while not sent:
                # loop 10 times, 30 seconds each, to give time for any pending transactions to be broadcast and free up UTXOs to build the next transaction
                # if it still doesn't go through after 300 seconds of pause, the wallet has probably run out of funds, or the blockchain is
                # ridiculously congested
                try:
                    self.wallet.transfer_multiple(
                        destinations, passphrase=passphrase)
                    sent = True
                except CannotCoverFee as e:
                    if attempts == 11:
                        raise CannotCoverFee(
                            "There is likely insufficient funds in your wallet to distribute the requested tokens."
                        ) from e

                    attempts += 1
                    time.sleep(30)
                except RESTServerError as e:
                    if attempts == 11:
                        raise RESTServerError(
                            "There are likely insufficient tokens in your wallet to distribute."
                        ) from e

                    attempts += 1
                    time.sleep(30)

    # last PROCESSED SLOT
    # format is blockno:indexno
    # returns tuple! make sure you match

#############################################################

    @staticmethod
    def dttodict(dt):
        return {"year": dt.year, "month": dt.month, "day": dt.day, "hour": dt.hour, "minute": dt.minute, "second": dt.second,  "mus": dt.microsecond}

    @staticmethod
    def dicttodt(dtdict):
        return datetime(dtdict['year'], dtdict['month'], dtdict['day'], dtdict['hour'], dtdict['minute'], dtdict['second'], dtdict['mus'])

    @staticmethod
    def isostringtodt(isostring):
        return datetime(
            int(isostring[:4]),
            int(isostring[5:7]),
            int(isostring[8:10]),
            int(isostring[11:13]),
            int(isostring[14:16]),
            int(isostring[17:19]),
            1,
        )

    def generateLog(self, initTokenbalance):
        timenow = datetime.utcnow()
        logdict = {"tokenBalance": [initTokenbalance], "txTime": [
            self.dttodict(timenow)], "slot": [1]}

        with open(self.logFile, 'w') as f:
            json.dump(logdict, f)

    def readLog(self):
        with open(self.logFile, 'r') as f:
            return json.load(f)

    def writeLog(self, logDict):
        with open(self.logFile, 'w') as f:
            json.dump(logDict, f)

    def rollback(self):
        logDict = self.readLog()
        logDict['txTime'].append(logDict['txTime'][-2])
        logDict['slot'].append(logDict['slot'][-2])
        self.writeLog(logDict)

    def readLastTime(self):
        logDict = self.readLog()
        return self.dicttodt(logDict['txTime'][-1])

    def writeLastTime(self, dt):
        logDict = self.readLog()
        logDict['txTime'].append(self.dttodict(dt))
        self.writeLog(logDict)

    def readSlot(self):
        logDict = self.readLog()
        return logDict['slot'][-1]

    def writeSlot(self, slot):
        logDict = self.readLog()
        logDict['slot'].append(slot)
        self.writeLog(logDict)

    def readAssetBalance(self):
        logDict = self.readLog()
        return logDict['tokenBalance'][-1]

    def writeAssetBalance(self, balance):
        logDict = self.readLog()
        logDict['tokenBalance'].append(balance)
        self.writeLog(logDict)

        print(f"TOKENS REM:  {str(balance)}\n")


###############################################################
    # save processed transactions to file

    @staticmethod
    def hexencode(utf8str):
        return utf8str.encode("utf-8").hex()

    @staticmethod
    def hexdecode(hexstr):
        return bytes.fromhex(hexstr).decode("utf-8")
