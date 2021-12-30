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
import random



#todo:find a neater way to pass in pullcost and pullprofit.
#write documentation

#instance variables:
#self.api: Blockfrost SDK Api object
#self.assetID: cardano.simpletypes AssetID object
#self.wallet: cardano.wallet wallet object
#self.faucetAddr: string of receiving address of faucet
#self.lastTxPosFile: str of filename of txt file that stores the block position of the last tx processed
#self.tokenTxFile: str of filename of txt file that stores the hashes of the incoming txs with tokens
class Faucet:

    #constructor
    #apiKey: str - Blockfrost Api Key
    #assetName: str - hex name of asset
    #assetPolicyID: str - policy id of asset
    #walletID: str - reference ID of wallet used by cardano-wallet
    #faucetAddr: str - receiving address of faucet
    #port: int - port that cardano-wallet is broadcasting on
    def __init__(self, apiKey,assetName, assetPolicyID, walletID, faucetAddr,pullcost=2000000, pullprofit=500000, proportionperpull=0.000015, port=8090):
        self.api = BlockFrostApi(project_id=apiKey)
        self.assetName = assetName
        self.assetPolicyID = assetPolicyID
        self.assetIDObj = AssetID(assetName,assetPolicyID)
        self.wallet = Wallet(walletID, backend=WalletREST(port=port))
        self.faucetAddr = faucetAddr
        self.bundlesize = None


        self.lastTxPosFile = assetName+assetPolicyID+faucetAddr+"pos.txt"
        self.tokenTxFile = assetName+assetPolicyID+faucetAddr+"tkn.txt"
        self.assetBalanceFile = assetName+assetPolicyID+faucetAddr+"balance.txt"
        self.PullsCountFile = assetName+assetPolicyID+faucetAddr+"pullscnt.txt"

        self.pullcost = pullcost
        self.proportionperpull = proportionperpull
        self.pullprofitraw = pullprofit
        self.pullprofit = Decimal(str(pullprofit/1000000))

        print("Faucet Created.\n")

    #generates files for the first time. every time the script is run, it draws from these files as
    #non-volatile memory.
    #run this once when setting up the faucet for the first time. It will
    #set a marker to disregard all transactions made prior to the time of running this method.
    def generateFiles(self, initTokenBalance, blockIndex=None, totalpulls=None):
        if blockIndex is None:
            blockIndex = int(self.api.block_latest().height)
        if totalpulls is None:
            totalpulls = 0
        self.writeIndex(blockIndex,0)
        self.writeAssetBalance(initTokenBalance)

    #loops for period seconds

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

    #multi utxo support (first n outputs to count): 1 for no multiutxo, singular output, 5 for first five utxos, 0 for unlimited
    #useful for testing throughput without wasting too much tx fees
    def runloop(self, passphrase, period=300,loops = 10000,bundlesize=20, multiutxo=1, multsallowed = 1):
        self.bundlesize = bundlesize
        ## TODO:
        #implement more intuitive ways to tweak the pullcost, pullprofit, proportionperpull  parameters. at the very least these should be passed in, not hardcoded
        #implement ways to define different pull yield distribution types (fixed value or normal distribution) and diminishing returns mode (fixed or proportional to faucet contents)

        starttime = datetime.now()

        for i in range(loops):
            try:
                print(f"LOOP:        {i+1}")
                timenow = datetime.now()
                timenowstr = timenow.strftime("%H:%M:%S")
                print(f"TIME:        {timenowstr}")
                timediff = timenow - starttime
                print(f"UPTIME:      {timediff}")
                self.sendtokens(passphrase,multiutxo=multiutxo)
                time.sleep(period)
            except ApiError:
                print("ERROR RECOVERY")
                self.rollbackIndex()
                time.sleep(3)
            finally:
                print("\n\n")

    #returns NFTtxlog
    #log file name in str form
    def sendtokens(self,passphrase, multiutxo: int = 1, multsallowed: int = 1):


        if multiutxo < 0:
            raise Exception("Multiutxo arg cannot be less than 0.")
        if multsallowed < 1 or (not isinstance(multsallowed, int)):
            raise Exception("Illegal multsallowed parameter.")

        try:
            lastblock, lastindex = self.readIndex()
            remainingtokens = self.getAssetBalance()
            lastbalance = remainingtokens
            currpullscount = self.getPullsCount()
        except FileNotFoundError:
            raise FileNotFoundError("You have not generated the blockchain index files. Please call generateFiles.")


        lastblocktxcount = None
        attempt = 0
        while lastblocktxcount is None:
            try:
                lastblocktxcount = self.api.block(str(lastblock)).tx_count
            except:
                attempt += 1
                print(f"Block fetch attempt {attempt} API Error - reattempting.")
                time.sleep(3)



        if lastindex == lastblocktxcount-1:
            from_block = str(lastblock+1)+":0"
        else:
            from_block = str(lastblock)+":"+str(lastindex+1)

        newtxs = self.api.address_transactions(address=self.faucetAddr, from_block=from_block)

        if len(newtxs) > 0:
            newlastblock = newtxs[-1].block_height
            newlastindex = newtxs[-1].tx_index
            self.writeIndex(newlastblock, newlastindex)

        #format of pendingTxList is [(senderaddr, pullyield(PERNIS), amountpaid(lovelace))]
        print(f"\nTOKENS CNT:  {lastbalance}")
        pendingTxList = []
        NFTtxlog = []
        badsends = 0
        yieldthisloop = 0
        numbersentthisloop = 0

        for tx in newtxs:
            txutxos = None
            attempt = 0
            while txutxos is None:
                try:
                    txutxos = self.api.transaction_utxos(hash=tx.tx_hash)
                except:
                    attempt += 1
                    print(f"UTXO fetch attempt {attempt} API Error - reattempting.")
                    time.sleep(3)

            #both of these are arrays
            txinputs = txutxos.inputs
            txoutputs = txutxos.outputs

            incomingtx = False
            outputshere = []
            containsNFTs = False
            #incoming or outgoing:
            for output in txoutputs:
                if not containsNFTs:
                    if output.address == self.faucetAddr:
                        incomingtx = True
                        #if has NFT:
                        if len(output.amount) > 1:
                            containsNFTs = True
                        else:
                            outputshere.append(int(output.amount[0].quantity))



            #outputshere contains array of integers, lovelace content for each utxo
            if incomingtx:
                if containsNFTs:
                    NFTtxlog.append(tx.tx_hash)
                else:
                    #multiutxo 1 means all utxos are treated as one chunks
                    #multiutxo 0 means all utxos are treated separately
                    #multiutxo 5 means utxos are treated separately up to a maximum of five chunks
                    #partone is the chunk where every output is considered
                    #parttwo is excess (to be returned)
                    partone = []
                    parttwo = []

                    if multiutxo == 0 or multiutxo >= len(outputshere):
                        partone = outputshere
                    else:
                        partone = outputshere[0:multiutxo]
                        parttwo = outputshere[multiutxo:]

                    #TODO: refactor pullprofit to be passed in, fix typing of profit
                    #note pendingList already takes out one copy of pullprofit

                    for utxoquant in partone:
                        if utxoquant >= self.pullcost:
                            validmults = min(multsallowed, utxoquant // self.pullcost)
                            returnada = utxoquant - ((validmults-1)*self.pullprofitraw)
                            randomyield = []

                            for i in range(validmults):
                                localyield = self.calculateYield(self.proportionperpull, remainingtokens)
                                randomyield.append(localyield)
                                remainingtokens -= localyield

                            pendingTxList.append((txinputs[0].address,sum(randomyield), returnada))
                            yieldthisloop += sum(randomyield)
                            numbersentthisloop += validmults

                    for utxoquant in parttwo:
                        if utxoquant >= self.pullcost:
                            badsends += 1
                            pendingTxList.append((txinputs[0].address,0, utxoquant + self.pullprofitraw))

        if len(pendingTxList)>0:
            self.autoSendAssets(pendingTxList, self.pullprofit, passphrase)

        if remainingtokens != (lastbalance-yieldthisloop):
            print(f"\nMismatch: Remtokens = {remainingtokens}, CalculatedBalance = {(lastbalance-yieldthisloop)}")

        print(f"TOKENS SENT: {str(yieldthisloop)}")
        self.writeAssetBalance(lastbalance-yieldthisloop)

        if len(NFTtxlog)>0:
            with open(self.tokenTxFile, 'a') as f:
                f.write(f"\n{str(NFTtxlog)}")


        print(f"No. Pulls:   {numbersentthisloop}")
        print(f"Bad Pulls:   {badsends}")
        self.writePullsCount(numbersentthisloop+currpullscount)



    #pendingTxList of format list of tuples of (senderaddr, pullyield, amountpaid)
    #
    #profit is how much less ada to lock to each utxo compared to the input amount
    #eg, for a locked ada difference of 0.4, a query of 2 will attach 1.6, whilst a query of 10 will attach 9.6

    #NOTE!!: pendingTxList's amountpaid is in lovelaces, offest is in ADA
    def autoSendAssets(self,pendingTxList, profit, passphrase):
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
                if pendingtx[1]!=0:
                    destinations.append((pendingtx[0], Decimal(str(pendingtx[-1]/1000000))-profit, [(self.assetIDObj,pendingtx[1])]))
                else:
                    destinations.append((pendingtx[0], Decimal(str(pendingtx[-1]/1000000))-profit))

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
    def readIndex(self):
        with open(self.lastTxPosFile, 'r') as f:
            lines = f.read().splitlines()
            last_line = lines[-1]
            zeroblockoneindex = last_line.split(":")
            return int(zeroblockoneindex[0]), int(zeroblockoneindex[1])

    #save processed transactions to file
    def writeIndex(self, blockno, txindex):
        with open(self.lastTxPosFile, 'a') as f:
            writestring = str(blockno)+":"+str(txindex)
            f.write(f"\n{writestring}")
        print(f"SLOT SAVED:  {writestring}")

    def rollbackIndex(self):
        with open(self.lastTxPosFile, 'r') as f:
            lines = f.read().splitlines()
            last_line = lines[-2]
            zeroblockoneindex = last_line.split(":")
        self.writeIndex(int(zeroblockoneindex[0]), int(zeroblockoneindex[1]))

    def getAssetBalance(self):
        with open(self.assetBalanceFile, 'r') as f:
            lines = f.read().splitlines()
            last_line = lines[-1]
            return int(last_line)

    #save processed transactions to file
    def writeAssetBalance(self, balance):
        with open(self.assetBalanceFile, 'a') as f:
            f.write(f"\n{str(balance)}")
        print(f"TOKENS REM:  {str(balance)}\n")

    def getPullsCount(self):
        with open(self.PullsCountFile, 'r') as f:
            lines = f.read().splitlines()
            last_line = lines[-1]
            return int(last_line)

    #save processed transactions to file
    def writePullsCount(self, balance):
        with open(self.PullsCountFile, 'a') as f:
            f.write(f"\n{str(balance)}")
        print(f"Tot. Pulls:  {str(balance)}")

    def calculateYield(self, proportionperpull, remainingtokens):
        return int(round(2*random.betavariate(12, 12)*int(round(remainingtokens*proportionperpull))))
