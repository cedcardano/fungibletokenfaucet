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

#TODO CREATE A SUPERCLASS FOR FAUCET AND SWAP
#Make print statements occur at the top level not the bottom
#make the write to log methods return instead of print directly
#make runloop able to access all the variables - maybe as in instance variable only used for logging
#so reliability isn't strict

#the faucet requires at least the very least 3*bundlesize ADA to function (more if your tokens are bundled with more than ~1.5ADA for each output),
#but throughput is greatly increased when there is more spare ADA in the wallet
#due to increased fragmentation and therefore there are lots of UTXOs to select from.
#throughput may be slow at first if your ADA is in a big chunk,
#but should increase as UTXOs are increasingly fragmented and the risk of contention is reduced.
#you can fragment the ADA yourself by breaking it into smaller UTXO chunks, but every distribution of 25 token UTXOs
#comes with 25 change UTXOs, so fragmentation should happen by itself fairly quickly.
#you can reduce bundlesize to reduce the minimum amount of ADA needed to operate, but this increases the number of transactions needed
# and thus fees. Do NOT increase bundlesize past 25 - higher amounts risk transactions failing due to exceeding size limitations.

#for maximum throughput I would recommend having at least 500ADA in the wallet, or even 2000+ if you want to
#loop 5 times a minute and approach Blockfrost's bottleneck of 500 tx per minute



class Faucet:

    #constructor
    #apiKey: str - Blockfrost Api Key
    #assetName: str - hex name of asset
    #assetPolicyID: str - policy id of asset
    #walletID: str - reference ID of wallet used by cardano-wallet
    #faucetAddr: str - receiving address of faucet
    #port: int - port that cardano-wallet is broadcasting on

    def __init__(self, apiKey,assetName, assetPolicyID, walletID, faucetAddr,pullcost=2000000, pullprofit=500000, proportionperpull=0.000015, port=8090, host="localhost", discord = False):
        self.api = BlockFrostApi(project_id=apiKey)
        self.assetName = assetName
        self.assetPolicyID = assetPolicyID
        self.assetIDObj = AssetID(assetName,assetPolicyID)
        self.wallet = Wallet(walletID, backend=WalletREST(port=port, host=host))
        self.faucetAddr = faucetAddr
        self.bundlesize = None

        self.logFile = assetName+assetPolicyID+faucetAddr+"v2.json"

        self.pullcost = Decimal(str(pullcost/1000000))
        self.proportionperpull = proportionperpull
        self.pullprofit = Decimal(str(pullprofit/1000000))

        #serve on port localhost port 5001
        self.discord = discord
        self.logging = True

        self.printiflog("Faucet Created.\n")

    def runloop(self, passphrase, period=300,loops = 10000,bundlesize=20, multsallowed = 1):
        self.bundlesize = bundlesize

        for i in range(loops):
            timenow = datetime.now()
            self.printiflog(f"SYS TIME:    {str(timenow)[:-7]}")


            self.sendtokens(passphrase, multsallowed=multsallowed)
            self.printiflog("___________________FAUCET___________________")
            time.sleep(period)

    def sendtokens(self,passphrase, multsallowed: int = 1):
        if multsallowed < 1 or (not isinstance(multsallowed, int)):
            raise Exception("Illegal multsallowed parameter.")
        try:
            remainingtokens = self.readAssetBalance()
            currpullscount = self.readPullsCount()
        except FileNotFoundError:
            raise FileNotFoundError("You have not generated the blockchain index files. Please call generateLog.")

        incomingtxs = self.get_new_incoming_txs()

        assetFilteredTxs = self.filtered_incomings_discard_assets(incomingtxs)
        senderaddrdict = self.get_sender_addr_dict([tx.txid for tx in assetFilteredTxs])
    
        sendlist, numpulls = self.prepare_sendlist(assetFilteredTxs, senderaddrdict, multsallowed, remainingtokens)
        if self.discord:
            sendlist += self.prepare_discord_topups()

        if len(sendlist)>0:
            total_send = self.autoSendAssets(sendlist, passphrase)
            self.printiflog(f"TOKENS SENT: {str(total_send)}")

            self.printiflog(f"No. Pulls:   {str(numpulls)}")
            self.writePullsCount(numpulls+currpullscount)

    def autoSendAssets(self,pendingTxList, passphrase):
        remainingtokens = self.readAssetBalance()
        total_send = sum(pendingtx['pullyield'] for pendingtx in pendingTxList)

        if self.bundlesize is None:
            self.bundlesize = 25
        groupsof = []
        smallarray = []

        #break into groups of bundlesize (25 by default) due to tx size constraints
        for i in range(len(pendingTxList)):
            if i == len(pendingTxList)-1:
                smallarray.append(pendingTxList[i])
                groupsof.append(smallarray)
            else:
                if len(smallarray)==self.bundlesize:
                    groupsof.append(smallarray)
                    smallarray = []
                smallarray.append(pendingTxList[i])

        for groupof in groupsof:
            destinations = []
            for pendingtx in groupof:
            #{"senderaddr": senderaddr, "pullyield": randomyield, "returnada": returnada}
                if pendingtx['pullyield']!=0:
                    destinations.append((pendingtx['senderaddr'], pendingtx['returnada'], [(self.assetIDObj,pendingtx['pullyield'])]))
                else:
                    destinations.append((pendingtx['senderaddr'], pendingtx['returnada']))

            attempts = 0
            sent = False
            while not sent:
                #loop 10 times, 30 seconds each, to give time for any pending transactions to be broadcast and free up UTXOs to build the next transaction
                #if it still doesn't go through after 300 seconds of pause, the wallet has probably run out of funds, or the blockchain is
                #ridiculously congested
                try:
                    outboundtx = self.wallet.transfer_multiple(destinations, passphrase=passphrase)
                    sent = True
                except CannotCoverFee:
                    if attempts == 11:
                        raise CannotCoverFee("There is likely insufficient funds in your wallet to distribute the requested tokens.")
                    attempts += 1
                    time.sleep(30)
                except RESTServerError:
                    if attempts == 11:
                        raise RESTServerError("There are likely insufficient tokens in your wallet to distribute.")
                    attempts += 1
                    time.sleep(30)

        self.writeAssetBalance(remainingtokens - total_send)
        return total_send

    def prepare_discord_topups(self) -> list[dict[str, str | int | Decimal]]:
        sessionsDict = requests.get("http://127.0.0.1:5001/sessions").json()
        appendPendingTxList = []
        for addr, topupAmount in sessionsDict.items():
            elemDict = {'senderaddr':addr, 'returnada': Decimal("1.5"),'pullyield':topupAmount }
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
                validmults = int(min(multsallowed, countedoutput.amount // self.pullcost))
                returnada = countedoutput.amount - validmults*self.pullprofit
                    
                randomyield = 0
                for i in range(validmults):
                    onetrial = self.calculateYield(self.proportionperpull, remainingtokens)
                    remainingtokens -= onetrial
                    randomyield += onetrial
                    
                sendlist.append({"senderaddr": senderaddr, "pullyield": randomyield, "returnada": returnada})
                    
                numpulls += validmults

            for output in extraoutputs:
                sendlist.append({"senderaddr": senderaddr, "pullyield": 0, "returnada": output.amount})

        return sendlist, numpulls

    def get_new_txs(self):
        try:
            lasttime = self.readLastTime()
            lastslot = self.readSlot()
        except FileNotFoundError:
            lastslot = 1
            lasttime = datetime.utcnow()

        currenttime = datetime.utcnow()
        self.printiflog(f"TIME INT:    {str(lasttime)[:-7]} to {str(currenttime)[:-7]}")
        newtxs = self.wallet.txsfiltered(lasttime)

        return newtxs, lastslot

    def get_new_incoming_txs(self):
        newtxs, lastslot = self.get_new_txs()

        if len(newtxs) == 0:
            return []

        incomingtxs = []
        for tx in newtxs:
            #local_inputs == [] means incoming transaction - these are necessarily confirmed already
            if tx.local_inputs == []:
                if tx.inserted_at.absolute_slot > lastslot:
                    incomingtxs.append(tx)
        
        if incomingtxs:
            newlastslot = newtxs[-1].inserted_at.absolute_slot
            newlasttime = self.isostringtodt(newtxs[-1].inserted_at.time)
            self.writeLastTime(newlasttime)
            self.writeSlot(newlastslot)

        return incomingtxs

    def filtered_incomings_discard_assets(self, incomingtxs):
        assetFilteredTxs = []
        for tx in incomingtxs:
            if not bool(sum(1 for output in list(tx.local_outputs) if output.assets != [])):
                assetFilteredTxs.append(tx)
        
        return assetFilteredTxs

    def get_sender_addr_dict(self, txid_list: list[str]) -> dict[str, str]:
        koiosrequest = self.koios_tx_utxos(txid_list)

        #dict of txid, addr
        if koiosrequest.status_code == 200:
            return {txdict['tx_hash']:txdict['inputs'][0]['payment_addr']['bech32'] for txdict in koiosrequest.json()}

        else:
            senderaddrdict = {}
            self.printiflog(f"Koios group request failed - reattempting individually.")
            #try for each, then blockfrost
            for txid in txid_list:
                koiosindivrequest = self.koios_tx_utxos([txid])                
                if koiosindivrequest.status_code == 200:
                    senderaddrdict[txid] = koiosindivrequest.json()[0]['inputs'][0]['payment_addr']['bech32']

                #blockfrost
                else:
                    self.printiflog(f"Koios individually request failed - reattempting with Blockfrost.")
                    senderaddress = None
                    attempt = 0
                    while senderaddress is None:
                        try:
                            senderaddress = self.api.transaction_utxos(hash=txid).inputs[0].address
                        except ApiError as e:
                            attempt += 1
                            self.printiflogprint(f"Blockfrost sender address fetch attempt {attempt} API Error {str(e.status_code)} - reattempting.")
                            time.sleep(3)
                    
                    senderaddrdict[txid] = senderaddress
            return senderaddrdict


    #last PROCESSED SLOT
    #format is blockno:indexno
    #returns tuple! make sure you match


    @staticmethod
    def koios_tx_utxos(txhashlist):
        koiosrequest = requests.post("https://api.koios.rest/api/v0/tx_utxos", json={"_tx_hashes":txhashlist})
        return koiosrequest

    def printiflog(self, printstring):
        if self.logging:
            print(printstring)

#############################################################
    @staticmethod
    def dttodict(dt):
        return {"year": dt.year, "month": dt.month, "day": dt.day,"hour": dt.hour, "minute": dt.minute, "second": dt.second,  "mus": dt.microsecond}

    @staticmethod
    def dicttodt(dtdict):
        return datetime(dtdict['year'],dtdict['month'],dtdict['day'],dtdict['hour'],dtdict['minute'],dtdict['second'],dtdict['mus'])

    @staticmethod
    def isostringtodt(isostring):
        return datetime(int(isostring[0:4]), int(isostring[5:7]), int(isostring[8:10]),int(isostring[11:13]),int(isostring[14:16]),int(isostring[17:19]),1)


    def generateLog(self, initTokenbalance, totalpulls):
        timenow = datetime.utcnow()
        logdict = {"tokenBalance": [initTokenbalance], "txTime": [self.dttodict(timenow)], "totalPulls": [totalpulls], "slot":[1]}

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
    #save processed transactions to file



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

    #constructor
    #apiKey: str - Blockfrost Api Key
    #assetName: str - hex name of asset
    #assetPolicyID: str - policy id of asset
    #walletID: str - reference ID of wallet used by cardano-wallet
    #faucetAddr: str - receiving address of faucet
    #port: int - port that cardano-wallet is broadcasting on

    def __init__(self, apiKey, receiveAssetName, receiveAssetPolicyID, sendAssetName,sendAssetPolicyID, walletID, swapperAddr,port=8090, host="localhost"):
        self.api = BlockFrostApi(project_id=apiKey)
        self.wallet = Wallet(walletID, backend=WalletREST(port=port, host=host))

        self.receiveAssetIDObj = AssetID(receiveAssetName,receiveAssetPolicyID)
        self.sendAssetIDObj = AssetID(sendAssetName,sendAssetPolicyID)

        self.swapperAddr = swapperAddr
        self.bundlesize = None

        self.logFile = receiveAssetName+receiveAssetPolicyID+sendAssetName+sendAssetPolicyID+swapperAddr+"swap.json"


        print("Token swap service tool created.\n")



    def runloop(self, passphrase, period=300,loops = 10000,bundlesize=20):
        self.bundlesize = bundlesize


        for i in range(loops):
            timenow = datetime.now()
            print(f"SYS TIME:    {str(timenow)[:-7]}")


            self.swaptokens(passphrase)
            print("____________________SWAP____________________")
            time.sleep(period)



    def swaptokens(self,passphrase):

        try:
            remainingtokens = self.readAssetBalance()
            lasttime = self.readLastTime()
            lastslot = self.readSlot()

        except FileNotFoundError:
            raise FileNotFoundError("You have not generated the blockchain index files. Please call generateLog.")


############################################################################################################

        currenttime = datetime.utcnow()
        print(f"TIME INT:    {str(lasttime)[:-7]} to {str(currenttime)[:-7]}")
        newtxs = self.wallet.txsfiltered(lasttime)

        incomingtxs = []
        for tx in newtxs:
            #local_inputs == [] means incoming transaction - these are necessarily confirmed already
            if tx.local_inputs == []:
                if tx.inserted_at.absolute_slot > lastslot:
                    incomingtxs.append(tx)


        if len(incomingtxs) > 0:
            newlastslot = incomingtxs[-1].inserted_at.absolute_slot
            newlasttime = self.isostringtodt(incomingtxs[-1].inserted_at.time)
            self.writeLastTime(newlasttime)
            self.writeSlot(newlastslot)

        sendlist=[]
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
                        senderaddr = self.api.transaction_utxos(hash=tx.txid).inputs[0].address
                    except ApiError as e:
                        attempt += 1
                        print(f"Sender address fetch attempt {attempt} API Error {str(e.status_code)} - reattempting.")
                        time.sleep(3)

                    sendlist.append({"senderaddr": senderaddr, "tokenpayload": totalcorrecttokens, "returnada": totalada})

                    tokensswapped += totalcorrecttokens



        if len(sendlist)>0:
            self.autoSendAssets(sendlist, passphrase)
            print(f"TKN SWAPPED: {str(tokensswapped)}")
            self.writeAssetBalance(remainingtokens-tokensswapped)


############################################################################

    def autoSendAssets(self,pendingTxList, passphrase):
        if self.bundlesize is None:
            self.bundlesize = 25
        groupsof = []
        smallarray = []

        #break into groups of bundlesize (25 by default) due to tx size constraints
        for i in range(len(pendingTxList)):
            if i == len(pendingTxList)-1:
                smallarray.append(pendingTxList[i])
                groupsof.append(smallarray)
            else:
                if len(smallarray)==self.bundlesize:
                    groupsof.append(smallarray)
                    smallarray = []
                smallarray.append(pendingTxList[i])

        for groupof in groupsof:
            destinations = []
            for pendingtx in groupof:
            #{"senderaddr": senderaddr, "tokenpayload": randomyield, "returnada": returnada}
                if pendingtx['tokenpayload']!=0:
                    destinations.append((pendingtx['senderaddr'], pendingtx['returnada'], [(self.sendAssetIDObj,pendingtx['tokenpayload'])]))
                else:
                    destinations.append((pendingtx['senderaddr'], pendingtx['returnada']))

            attempts = 0
            sent = False
            while not sent:
                #loop 10 times, 30 seconds each, to give time for any pending transactions to be broadcast and free up UTXOs to build the next transaction
                #if it still doesn't go through after 300 seconds of pause, the wallet has probably run out of funds, or the blockchain is
                #ridiculously congested
                try:
                    self.wallet.transfer_multiple(destinations, passphrase=passphrase)
                    sent = True
                except CannotCoverFee:
                    if attempts == 11:
                        raise CannotCoverFee("There is likely insufficient funds in your wallet to distribute the requested tokens.")
                    attempts += 1
                    time.sleep(30)
                except RESTServerError:
                    if attempts == 11:
                        raise RESTServerError("There are likely insufficient tokens in your wallet to distribute.")
                    attempts += 1
                    time.sleep(30)


    #last PROCESSED SLOT
    #format is blockno:indexno
    #returns tuple! make sure you match

#############################################################
    @staticmethod
    def dttodict(dt):
        return {"year": dt.year, "month": dt.month, "day": dt.day,"hour": dt.hour, "minute": dt.minute, "second": dt.second,  "mus": dt.microsecond}

    @staticmethod
    def dicttodt(dtdict):
        return datetime(dtdict['year'],dtdict['month'],dtdict['day'],dtdict['hour'],dtdict['minute'],dtdict['second'],dtdict['mus'])

    @staticmethod
    def isostringtodt(isostring):
        return datetime(int(isostring[0:4]), int(isostring[5:7]), int(isostring[8:10]),int(isostring[11:13]),int(isostring[14:16]),int(isostring[17:19]),1)


    def generateLog(self, initTokenbalance):
        timenow = datetime.utcnow()
        logdict = {"tokenBalance": [initTokenbalance], "txTime": [self.dttodict(timenow)], "slot":[1]}

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
    #save processed transactions to file



    @staticmethod
    def hexencode(utf8str):
        return utf8str.encode("utf-8").hex()

    @staticmethod
    def hexdecode(hexstr):
        return bytes.fromhex(hexstr).decode("utf-8")
