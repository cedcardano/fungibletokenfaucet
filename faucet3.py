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
import functools
import operator
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

        self.cardano_gql_api = CardanoGQL("https://cedric.app/api/dbsync/graphql/")

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
        txs_list = self.cardano_gql_api.txs(txid_list)
        return {txdict['hash']: txdict['inputs'][0]['address'] for txdict in txs_list}

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


class CardanoGQL:
    def __init__(self, apiurl):
        self.apiurl = apiurl

    def __get_cardano_gql_query(self, querystr, variables=None):
        sendjson = {"query": querystr}
        hdr = {"Content-Type": "application/json"}

        if variables:
            sendjson["variables"] = variables

        req = requests.post(self.apiurl, headers=hdr, json=sendjson)

        return req.json()

    @staticmethod
    def __fromHex(hexStr: str) -> str:
        return bytearray.fromhex(hexStr).decode()

    @staticmethod
    def __toHex(utfStr: str) -> str:
        return utfStr.encode("utf-8").hex()

    @staticmethod
    def __chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def addr_txs(self, payment_address, from_block=1):
        query = '''
                query addrTxs(
                    $address: String!
                    $fromBlock: Int!
                ) {
                    blocks (
                        where: { number : { _gte: $fromBlock}}
                    ){
                        number
                        transactions (where: {_or:[
                            {_or: {inputs:  {address:{_eq: $address}}}},
                            {_or: {outputs: {address:{_eq: $address}}}}
                        ]}) {
                            hash
                            inputs {
                                address
                                sourceTxIndex
                                sourceTxHash
                                value
                                tokens {
                                    asset {
                                        assetId
                                        assetName
                                        policyId
                                    }
                                    quantity
                                }       
                            }
                            outputs(order_by: { index: asc }) {
                                index
                                address
                                value
                                tokens {
                                    asset {
                                        assetId
                                        assetName
                                        policyId
                                    }
                                    quantity
                                }
                            }
                            metadata {
                                key
                                value
                            }
                        }
                    }
                }
                '''

        variables = {"address": payment_address, "fromBlock": from_block}
        req = self.__get_cardano_gql_query(query, variables)

        flat_req = [block['transactions']
                    for block in req['data']['blocks'] if block['transactions']]
        return list(functools.reduce(operator.add, flat_req)) if flat_req else []

    def addr_txs_full(self, payment_address):
        def paginate_txs(incoming_or_outgoing) -> set[str]:
            if incoming_or_outgoing == "i":
                query = '''
                    query addrTxs(
                        $address: String!
                        $limit: Int
                        $offset: Int
                    ) {
                        transactions (where: {outputs: {address:{_eq: $address}}}, limit: $limit, offset: $offset) {
                            hash
                        }
                    }
                    '''
            elif incoming_or_outgoing == "o":
                query = '''
                    query addrTxs(
                        $address: String!
                        $limit: Int
                        $offset: Int
                    ) {
                        transactions (where: {inputs: {address:{_eq: $address}}}, limit: $limit, offset: $offset) {
                            hash
                        }
                    }
                    '''

            anotherloop = True
            tx_hashes_set = set()
            numloops = 0
            while anotherloop:
                variables = {"address": payment_address,
                             "limit": 2500, "offset": numloops*2500}
                req = self.__get_cardano_gql_query(query, variables)
                returns_set = {tx['hash']
                               for tx in req['data']['transactions']}
                tx_hashes_set.update(returns_set)

                numloops += 1
                anotherloop = len(returns_set) == 2500

            return tx_hashes_set

        incoming_set = paginate_txs("i")
        outgoing_set = paginate_txs("o")

        return self.txs(list(incoming_set.union(outgoing_set)))

    def txs(self, tx_hash_list):
        query = '''
                query txs(
                    $hashes: [Hash32Hex]!
                ) {
                    transactions(
                        where: { hash: { _in: $hashes }}
                    ) {
                        hash
                        inputs {
                            address
                            sourceTxIndex
                            sourceTxHash
                            value
                            tokens {
                                asset {
                                    assetId
                                    assetName
                                    policyId
                                }
                                quantity
                            }                        
                        }
                        outputs(order_by: { index: asc }) {
                            index
                            address
                            value
                            tokens {
                                asset {
                                    assetId
                                    assetName
                                    policyId
                                }
                                quantity
                            }
                        }
                        metadata {
                            key
                            value
                        }
                    }
                }
                '''

        if len(tx_hash_list) <= 1000:
            variables = {"hashes": tx_hash_list}
            req = self.__get_cardano_gql_query(query, variables)
            return req['data']['transactions']

        first_part = tx_hash_list[:1000]
        variables = {"hashes": first_part}
        req = self.__get_cardano_gql_query(query, variables)
        first_part_txs = req['data']['transactions']

        return first_part_txs + self.txs(tx_hash_list[1000:])

    def chain_tip(self):
        query = '''
        { cardano { tip { number slotNo epoch { number } } } }
        '''
        return self.__get_cardano_gql_query(query)['data']

    def get_handle_addr(self, handle_name):
        if handle_name[0] == "$":
            handle_name = handle_name[1:]

        query = '''
            query assetHolders(
                $policyId: Hash28Hex!
                $assetName: Hex
            ) {
                utxos (limit: 1,
                    where: { tokens: { asset: { _and: [
                        {policyId: { _eq: $policyId}},
                        {assetName: { _eq: $assetName}}
                    ]}}}
                ) {
                    address
                }
            }
        '''
        variables = {"assetName": self.__toHex(
            handle_name), "policyId": "f0ff48bbb7bbe9d59a40f1ce90e9e9d0ff5002ec48f232b49ca0fb9a"}
        req = self.__get_cardano_gql_query(query, variables)

        return req['data']['utxos'][0]['address'] if req['data']['utxos'] else None
