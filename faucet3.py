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


class FifoLimDict(dict):
    def __init__(self, *args, **kwds):
        self.size_limit = kwds.pop("size", None)
        dict.__init__(self, *args, **kwds)
        self._check_size_limit()

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        self._check_size_limit()

    def _check_size_limit(self):
        if self.size_limit is not None:
            while len(self) > self.size_limit:
                self.pop(next(iter(self)))


class DbSyncPostgrestAPI:
    def __init__(self, listen_url: str):
        if listen_url[-1] != "/":
            listen_url += "/"
        self.listen_url = listen_url

        self.tx_cache = FifoLimDict(size=10000)

    def get_handle_addr(self, handle_name):
        if handle_name[0] == "$":
            handle_name = handle_name[1:]
        reqStr = f"ma_tx_out?select=id,tx_out!inner(id,address),multi_asset!inner(id,policy,name)&order=id.desc&limit=1&multi_asset.policy=eq.\\xf0ff48bbb7bbe9d59a40f1ce90e9e9d0ff5002ec48f232b49ca0fb9a&multi_asset.name=eq.\\x{self.__toHex(handle_name)}"

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

    def __get_tx_dbid_list(self, txid_list: list[str]) -> list[dict]:
        return self.__send_req("tx?hash=in.({})".format(','.join(['\\x'+str(txid) for txid in txid_list]))).json()

    def __get_tx_out_dbid_list(self, txdbid_list: list[int]) -> list[dict]:
        return self.__send_req(
            f"tx_out?tx_id=in.({','.join([str(txid) for txid in txdbid_list])})"
        ).json()

    def __get_ma_tx_out_dbid_list(self, tx_out_dbid_list: list[int]) -> list[dict]:
        return self.__send_req(
            f"ma_tx_out?tx_out_id=in.({','.join([str(txid) for txid in tx_out_dbid_list])})"
        ).json()

    def __get_multi_asset_dbid_list(self, list_idents: int) -> list[dict]:
        if list_idents:
            return self.__send_req(
                f"multi_asset?id=in.({','.join([str(ident) for ident in list_idents])})&select=id,policy,name"
            ).json()

        else:
            return []

    def __get_metadata_dbid_list(self, txdbid_list: list[int]) -> list[dict]:
        return self.__send_req(
            f"tx_metadata?tx_id=in.({','.join([str(txdbid) for txdbid in txdbid_list])})"
        ).json()

    def __get_tx_in_dbid_list(self, txdbid_list: list[int]) -> list[dict]:
        return self.__send_req(
            f"tx_in?tx_in_id=in.({','.join([str(txdbid) for txdbid in txdbid_list])})"
        ).json()

    def __get_tx_by_dbid_list(self, txdbid_list: list[int]) -> list[dict]:
        return self.__send_req(
            f"tx?id=in.({','.join([str(txdbid) for txdbid in txdbid_list])})"
        ).json()

    def __get_block_height_from_blockid(self, block_id_list: list):
        return self.__send_req(
            f"block?select=id,block_no&id=in.({','.join([str(blockid) for blockid in block_id_list])})"
        ).json()

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

    def __tx_info_raw(self, raw_list_txids: list[str]):
        already_cached = []
        list_txids = []

        for tx_hash in raw_list_txids:
            if tx_hash in self.tx_cache:
                already_cached.append(self.tx_cache[tx_hash])
            else:
                list_txids.append(tx_hash)

        if list_txids:
            try:
                tx_list = self.__get_tx_dbid_list(list_txids)
                tx_out_list = self.__get_tx_out_dbid_list(
                    (txdbids := [tx['id'] for tx in tx_list]))
                metadata_list = self.__get_metadata_dbid_list(txdbids)
                tx_ma_out_list = self.__get_ma_tx_out_dbid_list(
                    [tx_out['id'] for tx_out in tx_out_list])
                multi_asset_list = self.__get_multi_asset_dbid_list(
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

                multi_asset_list_by_dbid = {
                    ma['id']: ma for ma in multi_asset_list}

                tx_in_list = self.__get_tx_in_dbid_list(txdbids)
                tx_in_list_by_txdbid = {tx['id']: [] for tx in tx_list}
                for tx_in in tx_in_list:
                    tx_in_list_by_txdbid[tx_in['tx_in_id']].append(tx_in)

                tx_in_output_keys = [(txin['tx_out_id'], txin['tx_out_index'])
                                     for txin in tx_in_list]
                tx_in_out_list = list(filter(lambda txout: (txout['tx_id'], txout['index']) in tx_in_output_keys, self.__get_tx_out_dbid_list(
                    txin['tx_out_id'] for txin in tx_in_list)))
                tx_in_out_dict = {
                    (txout['tx_id'], txout['index']): txout for txout in tx_in_out_list}

                tx_in_tx_list = self.__get_tx_by_dbid_list(
                    [tx['tx_id'] for tx in tx_in_out_list])
                tx_in_tx_to_tx_hash = {tx['id']: self.__remove_slash_x(
                    tx['hash']) for tx in tx_in_tx_list}

                returnlist = []

                block_height_dict = {
                    block_pair['id']: block_pair['block_no']
                    for block_pair
                    in self.__get_block_height_from_blockid(
                        list({tx['block_id'] for tx in tx_list})
                    )
                }

                for tx in tx_list:
                    tx_returndict = {'tx_hash': self.__remove_slash_x(
                        tx['hash']), 'block_height': block_height_dict[tx['block_id']], 'tx_block_index': tx['block_index'], 'fee': tx['fee']}

                    outputs = tx_out_list_by_txdbid[tx['id']]
                    outputs_list = []
                    for output in outputs:
                        output_dict = {"payment_addr": {"bech32": output['address'], "cred": self.__remove_slash_x(
                            output["payment_cred"])}, "value": output['value'], "tx_index": output['index']}

                        tx_ma_outs = tx_ma_out_list_by_tx_out_dbid[output['id']]
                        assets_list = []
                        for tx_ma_out in tx_ma_outs:
                            asset_dict = {'quantity': tx_ma_out['quantity']}
                            multi_asset = multi_asset_list_by_dbid[tx_ma_out['ident']]
                            asset_dict['policy_id'] = self.__remove_slash_x(
                                multi_asset['policy'])
                            asset_dict['asset_name'] = self.__remove_slash_x(
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

                    inputs = tx_in_list_by_txdbid[tx['id']]
                    inputs_list = []
                    for input in inputs:
                        key_tuple = (input['tx_out_id'], input['tx_out_index'])
                        corresponding_output = tx_in_out_dict[key_tuple]

                        input_dict = {
                            'payment_addr': {
                                'bech32': corresponding_output['address'],
                                'cred': self.__remove_slash_x(
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
            except TypeError as e:
                return []
        else:
            returnlist = []
        for tx_dict in returnlist:
            self.tx_cache[tx_dict['tx_hash']] = tx_dict

        returnlist += already_cached

        return returnlist

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
