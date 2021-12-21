# fungibletokenfaucet
A token faucet for Cardano that runs on top of emesik's cardano-python module: https://github.com/emesik/cardano-python

Requires a local node (can use Daedalus' socket) and a cardano-wallet server on localhost. You will also need a Blockfrost API key, since this script is intended to not require cardano-db-sync.

# How-to
This file is intended to be run from Python Shell. It requires the same package installations as cardano-python - please ensure those are present in the venv.

1. Place the .py file in the cardano-python/cardano directory
2. Launch Python Shell in the cardano-python directory
3. Import the module with
```from cardano.faucet3 import *```
4. Ensure your wallet is known to cardano-wallet. You may import this (with a recovery phrase) using the cardano-wallet command line interface, or with cardano-python directly in the shell, or you may generate a new wallet. In particular, ensure you have a valid walletID (wid) and faucet address that belongs to the wallet.
5. Create your Faucet object - see documentation for parameters. A sample call may be

 ```faucet =  Faucet('apiKeyStr',"5045524e4953","184673879d2b78582141d8d6fefeb47879d27f370903335bd1e23792","walletID",'faucetaddr', 1800000,455202, 0.000015)```

6. If setting up the faucet for the first time, generate the blockchain index and remaining token files with the```generateFiles``` class method. Ensure that you enter the initial token balance as a parameter - this will be difficult to track with the API as the balance will become increasingly fragmented across many UTXOs and change addresses as the faucet runs. This will take a note of the current blockchain tip, and all subsequent incoming transactions to the faucet address will treated as potential faucet actuations. The files will now update themselves and will not need to be generated again for subsequent Faucet instances. As an example, if there are 1,000,000,000,000 tokens to be distributed, you can use

```faucet.generateFiles(1000000000000)```

7. Launch the faucet with ```runloop```, ensuring you enter your passphrase. Something like
```faucet.runloop("mypassphrase",period=300)```

# Parameter Constraints
todo: minimum 'buffer' ADA, beta distribution explanation, throughput and contention, selecting fees and profit, bundle size
