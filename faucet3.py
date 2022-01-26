from cardano.wallet import Wallet
from decimal import *
from cardano.backends.walletrest import WalletREST
from cardano.numbers import from_lovelaces
from cardano.simpletypes import AssetID
from cardano.backends.walletrest.exceptions import *
from cardano.exceptions import *
import getpass
import decimal
import math


def multi_send(wallet):
    logstring = ""
    assetDict = None
    

    destAddr = input("\nEnter the destination address: ")
    assetsToSendDict = {}
    minAda = False
    
    sendAmountPrompt = input("Enter ADA Amount to be sent, or type 'min' to calculate based on selected tokens: ")
    

    
    if sendAmountPrompt == "min":
        minAda = True
    else:
        try:       
            sendAmount = Decimal(str(round(float(sendAmountPrompt),6)))
        except decimal.InvalidOperation:
            return "Invalid ADA quantity. Please try again."
        except ValueError:
            return "Invalid ADA quantity. Please try again."

    addMoreTokens = True
    
    while addMoreTokens:      
        tokenPrompt = input("\nType a (case-sensitive) token name, or press enter if you are done adding tokens: ")
        assetIDObjGlob = None

        if tokenPrompt != "":
            if assetDict is None:
                assetDict = wallet.assets()
                hexAssetNames = [assetTuple[0].asset_name for assetTuple in assetDict.items()]
                assetNameCollisionDict = {}
                for assetIDObj in assetDict:
                    if assetIDObj.asset_name not in assetNameCollisionDict:
                        assetNameCollisionDict[assetIDObj.asset_name] = [assetIDObj.policy_id]
                    else:
                        assetNameCollisionDict[assetIDObj.asset_name].append(assetIDObj.policy_id)
            
            numNames = hexAssetNames.count(toHex(tokenPrompt))

            if numNames == 0:
                print("You do not own an asset with this name. Please try again.\n")              

            elif numNames > 1:
                print("There are multiple policy IDs with this token name:\n\n")
                for policyID in assetNameCollisionDict[toHex(tokenPrompt)]:
                    print(policyID)
                print("\n")

                specifiedPolicyID = input("Please enter the policy ID: ")
                if specifiedPolicyID not in assetNameCollisionDict[toHex(tokenPrompt)]:
                    print("Policy ID does not exist for this token name. Please try again.\n")
                else:
                    assetIDObjGlob = AssetID(toHex(tokenPrompt), specifiedPolicyID)

            else:
                assetIDObjGlob = AssetID(toHex(tokenPrompt), assetNameCollisionDict[toHex(tokenPrompt)][0])
        else:
            addMoreTokens = False
            
        if assetIDObjGlob is not None:

            tokenBalance = assetDict[assetIDObjGlob].available
            print(f"Token account balance: {str(tokenBalance)}")
            valid = True
            try:
                amountSend = int(input("Enter the number of tokens to send: "))
            except ValueError:
                print("The value you have entered is not valid.")
                valid = False            
            if amountSend < 0 or amountSend > tokenBalance:
                print("The value you have entered is not valid.")
                valid = False                   
            if valid:
                assetsToSendDict[assetIDObjGlob] = amountSend

    sequenceOfAssetPairs = []
    for assetIDObj, amountSend in assetsToSendDict.items():
        if amountSend != 0:
            sequenceOfAssetPairs.append((assetIDObj, amountSend))

    if minAda:
        if not sequenceOfAssetPairs:
            sendAmount = Decimal(1)
        else:
            numAssets = len(sequenceOfAssetPairs)
            sumAssetNameLengths = sum([len(assetIDObj.asset_name)/2 for assetIDObj, amountSend in sequenceOfAssetPairs])
            numPolicyIDs = len(list(dict.fromkeys([assetIDObj.policy_id for assetIDObj, amountSend in sequenceOfAssetPairs]))) 

            minBundledLovelace = 34482*(27+6+math.ceil((12*numAssets+sumAssetNameLengths+28*numPolicyIDs)/8))
            sendAmount = from_lovelaces(minBundledLovelace)

    msgMetadata = None
    msgMetadataString = input("\nType transaction message metadata, or Enter to skip: ")
    if msgMetadataString:
        msgMetadata = {674: {"msg": [msgMetadataString[i:i+64] for i in range(0, len(msgMetadataString), 64)]}}
  
    print("\nPROPOSED TRANSACTION")
    print(f"Destination:      {destAddr}")
    print(f"ADA to be sent:   {str(sendAmount)} ADA")

    if sequenceOfAssetPairs:
        destinations = [(destAddr, sendAmount, sequenceOfAssetPairs)]
    else:
        destinations = [(destAddr, sendAmount)]


    avgFee = estimate_fee_endpoint(destinations, msgMetadata, wallet)

    if avgFee is None:
        return logstring

    print(f"Estimated fee:    {str(avgFee)} ADA")
    print(f"Total ADA Cost:   {str(avgFee+sendAmount)} ADA")

    if msgMetadataString:
        print(f"\nMetadata:   {msgMetadataString}\n")

    if sequenceOfAssetPairs:
        print("           Tokens           ")

        proposedTokenTable = [[fromHex(assetIDObj.asset_name), str(balanceObj)] 
                            if hexAssetNames.count(assetIDObj.asset_name) == 1 
                            else [f"{fromHex(assetIDObj.asset_name)}:{assetIDObj.policy_id[:16]}", str(balanceObj)] 
                            for assetIDObj, balanceObj in assetsToSendDict.items()
                            if balanceObj > 0]
        proposedTokenTable.insert(0,["TOKEN", "AMOUNT"])
        col_width = max(len(word) for row in proposedTokenTable for word in row) + 10
        proposedTokenTable.insert(1,["-"*col_width, "-"*col_width])
        

        for row in proposedTokenTable:
            print("".join(word.ljust(col_width) for word in row))
        
        print("\n")

        

    passphrase = getpass.getpass("Enter your passphrase to send, or type 'cancel' to abort: ")
    if passphrase == "cancel":
        return "Transaction aborted. Funds were not sent."

    params =    {
                    "destinations"      : destinations,
                    "metadata"          : msgMetadata,
                    "allow_withdrawal"  : True,
                    "ttl"               : None,
                    "passphrase"        : passphrase
                }
    
    tx = send_endpoint(params, wallet)
    if tx:
        logstring += f"Transaction {tx.txid} has been sent.\n\n"
        logstring += f"https://cardanoscan.io/transaction/{tx.txid}"

    return logstring


def basic_send(wallet):
    logstring = ""
    assetDict = None
    

    destAddr = input("\nEnter the destination address: ")
    assetsToSendDict = {}
    minAda = False
    
    sendAmountPrompt = input("Enter ADA Amount to be sent, or type 'min' to calculate based on selected tokens: ")
    

    
    if sendAmountPrompt == "min":
        minAda = True
    else:
        try:       
            sendAmount = Decimal(str(round(float(sendAmountPrompt),6)))
        except decimal.InvalidOperation:
            return "Invalid ADA quantity. Please try again."
        except ValueError:
            return "Invalid ADA quantity. Please try again."

    addMoreTokens = True
    
    while addMoreTokens:      
        tokenPrompt = input("\nType a (case-sensitive) token name, or press enter if you are done adding tokens: ")
        assetIDObjGlob = None

        if tokenPrompt != "":
            if assetDict is None:
                assetDict = wallet.assets()
                hexAssetNames = [assetTuple[0].asset_name for assetTuple in assetDict.items()]
                assetNameCollisionDict = {}
                for assetIDObj in assetDict:
                    if assetIDObj.asset_name not in assetNameCollisionDict:
                        assetNameCollisionDict[assetIDObj.asset_name] = [assetIDObj.policy_id]
                    else:
                        assetNameCollisionDict[assetIDObj.asset_name].append(assetIDObj.policy_id)
            
            numNames = hexAssetNames.count(toHex(tokenPrompt))

            if numNames == 0:
                print("You do not own an asset with this name. Please try again.\n")              

            elif numNames > 1:
                print("There are multiple policy IDs with this token name:\n\n")
                for policyID in assetNameCollisionDict[toHex(tokenPrompt)]:
                    print(policyID)
                print("\n")

                specifiedPolicyID = input("Please enter the policy ID: ")
                if specifiedPolicyID not in assetNameCollisionDict[toHex(tokenPrompt)]:
                    print("Policy ID does not exist for this token name. Please try again.\n")
                else:
                    assetIDObjGlob = AssetID(toHex(tokenPrompt), specifiedPolicyID)

            else:
                assetIDObjGlob = AssetID(toHex(tokenPrompt), assetNameCollisionDict[toHex(tokenPrompt)][0])
        else:
            addMoreTokens = False
            
        if assetIDObjGlob is not None:

            tokenBalance = assetDict[assetIDObjGlob].available
            print(f"Token account balance: {str(tokenBalance)}")
            valid = True

            amountSendPrompt = input("Enter the number of tokens to send, or type 'max' to send all tokens of this type: ")

            if amountSendPrompt == 'max':
                amountSend = tokenBalance
            else:
                try:
                    amountSend = int(input(amountSendPrompt))
                except ValueError:
                    print("The value you have entered is not valid.")
                    valid = False            
                if amountSend < 0 or amountSend > tokenBalance:
                    print("The value you have entered is not valid.")
                    valid = False                   
            if valid:
                assetsToSendDict[assetIDObjGlob] = amountSend

    sequenceOfAssetPairs = []
    for assetIDObj, amountSend in assetsToSendDict.items():
        if amountSend != 0:
            sequenceOfAssetPairs.append((assetIDObj, amountSend))

    if minAda:
        if not sequenceOfAssetPairs:
            sendAmount = Decimal(1)
        else:
            numAssets = len(sequenceOfAssetPairs)
            sumAssetNameLengths = sum([len(assetIDObj.asset_name)/2 for assetIDObj, amountSend in sequenceOfAssetPairs])
            numPolicyIDs = len(list(dict.fromkeys([assetIDObj.policy_id for assetIDObj, amountSend in sequenceOfAssetPairs]))) 

            minBundledLovelace = 34482*(27+6+math.ceil((12*numAssets+sumAssetNameLengths+28*numPolicyIDs)/8))
            sendAmount = from_lovelaces(minBundledLovelace)

    msgMetadata = None
    msgMetadataString = input("\nType transaction message metadata, or Enter to skip: ")
    if msgMetadataString:
        msgMetadata = {674: {"msg": [msgMetadataString[i:i+64] for i in range(0, len(msgMetadataString), 64)]}}
  
    print("\nPROPOSED TRANSACTION")
    print(f"Destination:      {destAddr}")
    print(f"ADA to be sent:   {str(sendAmount)} ADA")

    if sequenceOfAssetPairs:
        destinations = [(destAddr, sendAmount, sequenceOfAssetPairs)]
    else:
        destinations = [(destAddr, sendAmount)]


    avgFee = estimate_fee_endpoint(destinations, msgMetadata, wallet)

    if avgFee is None:
        return logstring

    print(f"Estimated fee:    {str(avgFee)} ADA")
    print(f"Total ADA Cost:   {str(avgFee+sendAmount)} ADA")

    if msgMetadataString:
        print(f"\nMetadata:   {msgMetadataString}\n")

    if sequenceOfAssetPairs:
        print("           Tokens           ")

        proposedTokenTable = [[fromHex(assetIDObj.asset_name), str(balanceObj)] 
                            if hexAssetNames.count(assetIDObj.asset_name) == 1 
                            else [f"{fromHex(assetIDObj.asset_name)}:{assetIDObj.policy_id[:16]}", str(balanceObj)] 
                            for assetIDObj, balanceObj in assetsToSendDict.items()
                            if balanceObj > 0]
        proposedTokenTable.insert(0,["TOKEN", "AMOUNT"])
        col_width = max(len(word) for row in proposedTokenTable for word in row) + 10
        proposedTokenTable.insert(1,["-"*col_width, "-"*col_width])
        

        for row in proposedTokenTable:
            print("".join(word.ljust(col_width) for word in row))
        
        print("\n")

        

    passphrase = getpass.getpass("Enter your passphrase to send, or type 'cancel' to abort: ")
    if passphrase == "cancel":
        return "Transaction aborted. Funds were not sent."

    params =    {
                    "destinations"      : destinations,
                    "metadata"          : msgMetadata,
                    "allow_withdrawal"  : True,
                    "ttl"               : None,
                    "passphrase"        : passphrase
                }
    
    tx = send_endpoint(params, wallet)
    if tx:
        logstring += f"Transaction {tx.txid} has been sent.\n\n"
        logstring += f"https://cardanoscan.io/transaction/{tx.txid}"

    return logstring



def send_all(wallet):
    logstring = ""
    assetDict = wallet.assets()

    balance = wallet.balance().total

    destAddr = input("\nEnter the destination address: ")

    sequenceOfAssetPairs = []
    for assetIDObj, amountSend in assetDict.items():
        if amountSend.available != 0:
            sequenceOfAssetPairs.append((assetIDObj, amountSend.available))

    msgMetadata = None
    msgMetadataString = input("\nType transaction message metadata, or Enter to skip: ")
    if msgMetadataString:
        msgMetadata = {674: {"msg": [msgMetadataString[i:i+64] for i in range(0, len(msgMetadataString), 64)]}}
  
    if sequenceOfAssetPairs:
        destinations = [(destAddr, balance, sequenceOfAssetPairs)]
    else:
        destinations = [(destAddr, balance)]

    avgFee = estimate_fee_endpoint(destinations, msgMetadata, wallet)

    if avgFee is None:
        return logstring

    sendAmount = balance - avgFee

    if sequenceOfAssetPairs:
        destinations = [(destAddr, sendAmount, sequenceOfAssetPairs)]
    else:
        destinations = [(destAddr, sendAmount)]

    print("\nPROPOSED TRANSACTION")
    print(f"Destination:      {destAddr}")
    print(f"ADA to be sent:   {str(sendAmount)} ADA")
    print(f"Estimated fee:    {str(avgFee)} ADA")
    print(f"Total ADA Cost:   {str(avgFee+sendAmount)} ADA")

    if msgMetadataString:
        print(f"\nMetadata:   {msgMetadataString}\n")

    if sequenceOfAssetPairs:

        hexAssetNames = [assetTuple[0].asset_name for assetTuple in assetDict.items()]

        print("           Tokens           ")

        proposedTokenTable = [[fromHex(assetIDObj.asset_name), str(balanceObj.available)] 
                            if hexAssetNames.count(assetIDObj.asset_name) == 1 
                            else [f"{fromHex(assetIDObj.asset_name)}:{assetIDObj.policy_id[:16]}", str(balanceObj.available)] 
                            for assetIDObj, balanceObj in assetDict.items()
                            if balanceObj.available > 0]
        proposedTokenTable.insert(0,["TOKEN", "AMOUNT"])
        col_width = max(len(word) for row in proposedTokenTable for word in row) + 10
        proposedTokenTable.insert(1,["-"*col_width, "-"*col_width])
        

        for row in proposedTokenTable:
            print("".join(word.ljust(col_width) for word in row))
        
        print("\n")


    passphrase = getpass.getpass("YOU ARE SENDING ALL ADA AND TOKENS IN YOUR WALLET.\n\nEnter your passphrase to send, or type 'cancel' to abort: ")
    if passphrase == "cancel":
        return "Transaction aborted. Funds were not sent."

    params =    {
                    "destinations"      : destinations,
                    "metadata"          : msgMetadata,
                    "allow_withdrawal"  : True,
                    "ttl"               : None,
                    "passphrase"        : passphrase
                }
    
    tx = send_endpoint(params, wallet)
    if tx:
        logstring += f"Transaction {tx.txid} has been sent.\n\n"
        logstring += f"https://cardanoscan.io/transaction/{tx.txid}"

    return logstring

def estimate_fee_endpoint(destinations, metadata, wallet):
    try:
        avgFeeTuple = wallet.estimate_fee(destinations, metadata = metadata)
        return avgFeeTuple[0]

    except BadRequest as e:
        print(e)
        print("\nA destination address is likely malformed. Please try again.")
    except CannotCoverFee as e:
        print(e)
        print("\nYou likely do not have enough funds to complete the transaction. Please adjust your values and try again.")
    except RESTServerError as e:
        print(e)        
        print("\nYou likely do not have enough funds to complete the transaction. Please adjust your values and try again.")
    return None

def send_endpoint(paramsDict, wallet):
    try:
        tx = wallet.transfer_multiple(destinations = paramsDict['destinations'], 
                                            metadata = paramsDict['metadata'],
                                            allow_withdrawal = paramsDict['allow_withdrawal'],
                                            ttl = paramsDict['ttl'],
                                            passphrase = paramsDict['passphrase']
                                        )
        return tx
    except BadRequest as e:
        print(e)
        print("\nA destination address is likely malformed. Please try again.")
    except CannotCoverFee as e:
        print(e)
        print("\nYou likely do not have enough funds to complete the transaction. Please adjust your values and try again.")
    except RESTServerError as e:
        print(e)
        if str(e)[0:33] == "The given encryption passphrase d":
            print("\nYour passphrase is incorrect. Please try again.")
            passphrase = getpass.getpass("Enter your passphrase: ")

            copyParamsDict = paramsDict
            copyParamsDict['passphrase'] = passphrase
            return send_endpoint(copyParamsDict, wallet)

        else:
            print("\nYou likely do not have enough funds to complete the transaction. Please adjust your values and try again.")
    
    return None


def receive(wallet):
    logstring = ""

    try:
        logstring += str(wallet.localFirstAddress)
    except AttributeError:
        wallet.localFirstAddress = wallet.addresses()[0]
        logstring += str(wallet.localFirstAddress)

    return logstring

def show_ada_balance(wallet):
    logstring = "BALANCE\n"

    balance = wallet.balance()
    totalAda = str(balance.total)
    availAda = str(balance.available)
    rewardAda = str(balance.reward)

    logstring += "TOTAL: " + totalAda + " ADA\n"
    logstring += "AVAIL: " + availAda + " ADA\n"
    logstring += "RWARD: " + rewardAda+ " ADA"
    return logstring

def list_assets(wallet, assetdict = None):
    logstring = ""

    if assetdict is None:
        assetdict = wallet.assets()
        
    hexAssetNames = [assetTuple[0].asset_name for assetTuple in assetdict.items()]

    nameBalanceTable = [[fromHex(assetIDObj.asset_name), str(balanceObj.available)] if hexAssetNames.count(assetIDObj.asset_name) == 1 else [f"{fromHex(assetIDObj.asset_name)}:{assetIDObj.policy_id[:16]}", str(balanceObj.available)] for assetIDObj, balanceObj in assetdict.items()]
    nameBalanceTable.insert(0,["TOKEN", "AMOUNT"])
    col_width = max(len(word) for row in nameBalanceTable for word in row) + 10
    nameBalanceTable.insert(1,["-"*col_width, "-"*col_width])
    

    for row in nameBalanceTable:
        logstring += ("".join(word.ljust(col_width) for word in row)) + "\n"

    return logstring[:-1]

def show_menu_options(wallet):
    menuStr = "MENU OPTIONS\n"

    for selectStr, mappedFunction in functionMap.items():
        menuStr += f"{selectStr}: {mappedFunction.__name__}\n"
    
    return(menuStr[:-1])


################################################

def set_function_map():
    global functionMap
    functionMap =  {"1":show_ada_balance,
                    "2":list_assets,
                    "3":receive,
                    "4":basic_send,
                    "5":send_all,
                    "0":show_menu_options}

################################################

#returns function object - still must be called
def get_matched_function(selectionStr):
    return functionMap[selectionStr]

def get_user_selection():
    menuStr = "\nMenu select: "
    return input(menuStr)

#####################################################

def fromHex(hexStr):
    return bytearray.fromhex(hexStr).decode()

def toHex(utfstring):
    return utfstring.encode("utf-8").hex()

#####################################################

if __name__ == "__main__":
    set_function_map()

    wid = input("Enter your wid: ")
    port = int(input("Enter your cardano-wallet port: "))

    wallet = Wallet(wid, backend = WalletREST(port=port))

    print("\n"+show_ada_balance(wallet))
    print("\n"+show_menu_options(wallet))

    #event loop
    while True:
        matchedFunct = None
        selectionStr = get_user_selection()
        try:
            matchedFunct = get_matched_function(selectionStr)
        except KeyError:
            print("Selection does not exist in the menu. Please try again.")

        if matchedFunct is not None:
            logString = matchedFunct(wallet)

            print(f"\n{logString}")
