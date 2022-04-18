from cardano.wallet import Wallet
from cardano.wallet import WalletService
from cardano.backends.walletrest import WalletREST
from cardano.simpletypes import AssetID
from cardano.exceptions import CannotCoverFee
from cardano.backends.walletrest.exceptions import RESTServerError
from decimal import *
import time
from datetime import datetime
import random
import json
import requests
from datetime import timezone


class Faucet:

    def __init__(self, apiKey, assetName, assetPolicyID, walletID, faucetAddr, pullcost=2000000, pullprofit=500000, proportionperpull=0.000015, port=8090, host="localhost", discord=False):
        self.assetName = assetName
        self.assetPolicyID = assetPolicyID
        self.assetIDObj = AssetID(assetName, assetPolicyID)
        self.wallet = Wallet(
            walletID, backend=WalletREST(port=port, host=host))
        self.faucetAddr = faucetAddr
        self.bundlesize = None

        self.db_api = DbSyncPostgrestAPI(
            "https://cedric.app/api/dbsync/postgrest/")

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
        discord_topup = False
        if self.discord and (prepare_topups := self.prepare_discord_topups()):
            sendlist += prepare_topups
            discord_topup = True

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
        def in_dict():
            return sum((tx in tx_addr_dict) for tx in txid_list) == len(txid_list)

        tx_addr_dict = {}
        while not in_dict():
            txs_list = self.db_api.tx_info(txid_list)
            tx_addr_dict = {txdict['tx_hash']: txdict['inputs'][0]
                            ['payment_addr']['bech32'] for txdict in txs_list}

        return tx_addr_dict

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

class DbSyncPostgrestAPI:
    def __init__(self, listen_url: str):
        if listen_url[-1] != "/":
            listen_url += "/"
        self.listen_url = listen_url

    def get_handle_addr(self, handle_name):
        if handle_name[0] == "$":
            handle_name = handle_name[1:]
        reqStr = (
            "ma_tx_out" +
            "?select=id,tx_out!inner(id,address),multi_asset!inner(id,policy,name)" +
            "&order=id.desc" +
            "&limit=1" +
            "&multi_asset.policy=eq.\\xf0ff48bbb7bbe9d59a40f1ce90e9e9d0ff5002ec48f232b49ca0fb9a" +
            f"&multi_asset.name=eq.\\x{self.__toHex(handle_name)}"
        )
        try:
            resp = self.__send_req(reqStr).json()
            return resp[0]["tx_out"]["address"]
        except:
            return None

    def tx_info(self, list_txids: list[str]):
        return self.__tx_info(list_txids)

    def __send_req(self, url_payload: str) -> requests.Response:
        return requests.get(self.listen_url + url_payload)

    @staticmethod
    def __fromHex(hexStr: str) -> str:
        return bytearray.fromhex(hexStr).decode()

    @staticmethod
    def __toHex(utfStr: str) -> str:
        return utfStr.encode("utf-8").hex()

    @staticmethod
    def __remove_slash_x(raw_hex_str: str) -> str:
        return raw_hex_str[2:]

    @staticmethod
    def __chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def __tx_info(self, raw_list_txids: list[str], order: str = "asc"):
        if not raw_list_txids:
            return []
        chunked_txids = list(self.__chunks(raw_list_txids, 100))

        if isinstance(raw_list_txids, str):
            r = self.__tx_info([raw_list_txids])
            return r[0] if r else []

        return_arr = []

        for chunk in chunked_txids:
            return_arr.extend(self.__tx_info_raw(chunk))

        return sorted(return_arr, key=lambda tx: (tx['block_height'], tx['tx_block_index']), reverse=(order == "desc"))

    def __tx_info_raw(self, list_txids: list[str]):
        tx_ids_str = '(' + ",".join(list(map(lambda hash: '\\x' + hash, list_txids))) + ')'

        response_txs_info = self.__send_req(
            "tx" +
            "?select=hash,block!inner(block_no),block_index,fee"
            f"&hash=in.{tx_ids_str}"
        ).json()

        response_txs_metadata = self.__send_req(
            "tx_metadata" +
            "?select=key,json,tx!inner(hash)" +
            f"&tx.hash=in.{tx_ids_str}"
        ).json()

        response_outputs_ma_outs = self.__send_req(
            "ma_tx_out" +
            "?select=tx_out!inner(tx!inner(hash),index),multi_asset!inner(policy,name),quantity"
            f"&tx_out.tx.hash=in.{tx_ids_str}"
        ).json()

        response_outputs_info = self.__send_req(
            "tx_out" +
            "?select=tx!inner(hash),index,address,payment_cred,value"
            f"&tx.hash=in.{tx_ids_str}"
        ).json()

        response_inputs_info = self.__send_req(
            "tx_in" +
            "?select=inputs:tx!tx_in_tx_out_id_fkey!inner(hash,tx_out!inner(index,address,payment_cred,value)),tx_out_index,outputs:tx!tx_in_tx_in_id_fkey!inner(hash)" +
            f"&outputs.hash=in.{tx_ids_str}"
        ).json()

        if 'code' in response_txs_info:
            return []

        txs_info = {}
        for dct in response_txs_info:
            tx_hash = self.__remove_slash_x(dct["hash"])
            txs_info[tx_hash] = {
                "tx_hash": tx_hash,
                "block_height": dct["block"]["block_no"],
                "tx_block_index": dct["block_index"],
                "fee": dct["fee"]
            }

        txs_metadata = {}
        for dct in response_txs_metadata:
            tx_hash = self.__remove_slash_x(dct["tx"]["hash"])
            if tx_hash not in txs_metadata:
                txs_metadata[tx_hash] = []
            txs_metadata[tx_hash].append(
                {
                    "key": dct["key"],
                    "json": dct["json"]
                }
            )

        outputs_tokens = {}
        for dct in response_outputs_ma_outs:
            idx = dct["tx_out"]["index"]
            tx_hash = self.__remove_slash_x(dct["tx_out"]["tx"]["hash"])
            if (tx_hash, idx) not in outputs_tokens:
                outputs_tokens[(tx_hash, idx)] = []
            outputs_tokens[(tx_hash, idx)].append({
                "policy_id": self.__remove_slash_x(dct["multi_asset"]["policy"]),
                "asset_name": self.__remove_slash_x(dct["multi_asset"]["name"]),
                "quantity": dct["quantity"]
            })

        outputs_info = {}
        for dct in response_outputs_info:
            idx = dct['index']
            tx_hash = self.__remove_slash_x(dct["tx"]["hash"])
            if tx_hash not in outputs_info:
                outputs_info[tx_hash] = []
            outputs_info[tx_hash].append({
                "payment_addr": {
                    "bech32": dct["address"],
                    "cred": self.__remove_slash_x(dct["payment_cred"])
                },
                "tx_index": idx,
                "value": dct["value"]
            })

        inputs_info = {}
        for dct in response_inputs_info:
            output_tx_hash = self.__remove_slash_x(dct['outputs']['hash'])
            if output_tx_hash not in inputs_info:
                inputs_info[output_tx_hash] = []

            matching_input = next(filter(
                lambda out_dict: out_dict['index'] == dct['tx_out_index'], dct['inputs']['tx_out']))
            inputs_info[output_tx_hash].append(
                {
                    "payment_addr": {
                        "bech32": matching_input['address'],
                        "cred": self.__remove_slash_x(matching_input['payment_cred'])
                    },
                    "tx_hash": self.__remove_slash_x(dct['inputs']['hash']),
                    "tx_index": dct['tx_out_index'],
                    "value": matching_input['value']
                }
            )

        return_array = []
        for tx_hash, tx_info in txs_info.items():
            tx_info['inputs'] = inputs_info[tx_hash]
            outputs_list = outputs_info[tx_hash]
            for output in outputs_list:
                output['asset_list'] = outputs_tokens[(tx_hash, output['tx_index'])] if (
                    tx_hash, output['tx_index']) in outputs_tokens else []
            tx_info['outputs'] = outputs_list
            tx_info['metadata'] = txs_metadata[tx_hash] if tx_hash in txs_metadata else []
            return_array.append(tx_info)
        return return_array

    def address_txs(self, addr: str, from_block: int = None, to_block: int = None, order: str = "asc"):
        from_block_str = f"&block.block_no=gte.{from_block}" if from_block else ""
        to_block_str = f"&block.block_no=lte.{to_block}" if to_block else ""

        routputs = self.__send_req(
            "tx?select=hash," +
            "outputs:tx_out!inner(index)," +
            "block!inner(block_no)" +
            from_block_str +
            to_block_str +
            f"&tx_out.address=eq.{addr}").json()

        rinputs = self.__send_req(
            "tx?select=hash," +
            "tx_in!tx_in_tx_in_id_fkey!inner(tx_out_index,tx!tx_in_tx_out_id_fkey!inner(hash,tx_out!inner(index, address)))," +
            "block!inner(block_no)" +
            from_block_str +
            to_block_str +
            f"&tx_in.tx.tx_out.address=eq.{addr}"
        ).json()

        routputs_list = [self.__remove_slash_x(tx['hash']) for tx in routputs]
        rinputs_list = [
            self.__remove_slash_x(tx['hash'])
            for tx in filter(
                lambda tx_dict: bool(
                    sum(
                        tx_in['tx_out_index']
                        in [tx_out['index'] for tx_out in tx_in['tx']['tx_out']]
                        for tx_in in tx_dict['tx_in']
                    )
                ),
                rinputs,
            )
        ]

        # note that routputs corresponds to INCOMING txs (as the address is in the outputs of the tx)
        return self.__tx_info(rinputs_list+routputs_list, order), routputs_list, rinputs_list
