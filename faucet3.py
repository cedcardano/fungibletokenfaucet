import discord
from discord.ext import commands, tasks
import json
from cardano.wallet import Wallet
from cardano.backends.walletrest import WalletREST
from cardano.simpletypes import AssetID
from cardano.exceptions import CannotCoverFee
from cardano import transaction as cardanotransaction
from cardano.backends.walletrest.exceptions import RESTServerError, BadRequest
import random
from decimal import Decimal
import asyncio
import time
import requests
import os
import math
from datetime import datetime, timedelta
import qrcode
from mee6_py_api.api import API
import typing
import functools
import operator
from datetime import timezone

# to do list
# 3. lottery
# 4. improved tracing for converters
# 5. withdraw specific amount of assets
# 6. dm users when tipped (with invite) if they're not in the server


class Tip(commands.Cog):
    """
    Entry point class for the tipbot functionality.
    """

    def __init__(self, client):
        """Constructor. Sets hardcoded instance variables.

        Args:
            client (discord.Client): Discord client instance.
        """

        self.client = client

        self.mee6_api = API(920580089723383818)
        self.xpperpull = 2400

        self.cgql_api = CardanoGQL("https://cedric.app/api/dbsync/graphql/")
        
        self.wallet = Wallet(
            "ba4707c844a378dc96949e4872d72fbbd5c61cbd", backend=WalletREST(port=8089, host="192.168.20.12"))
        self.faucetwallet = Wallet(
            "cd1ec3214e500da26e7e39aa3516faabe96edf1d", backend=WalletREST(port=8089, host="192.168.20.12"))

        self.user_manager = UserManager(self.wallet)
        self.tipping_backend = TippingBackend(self.wallet, self.user_manager)

        self.knownassets: dict[str, tuple[str, str]] = {
            "PERNIS": ("37b47fcaeb067582eb0b4230632633adffa7753481139b67cc8fe3ce", self.toHex("PERNIS")),
            "HOSKY":  ("a0028f350aaabe0545fdcb56b039bfb08e4bb4d8c4d7c3c7d481c235", self.toHex("HOSKY"))
        }

        self.withdraw_pool: list[tuple[str, Decimal,
                                       list[tuple[AssetID, int]]] | tuple[str, Decimal]] = []

        if os.path.isfile('./__config/txs_processed.json'):
            with open("./__config/txs_processed.json", 'r', encoding='utf-8') as f:
                txs_processed: list[str] = json.load(f)
            self.txs_processed = txs_processed
        else:
            self.txs_processed: list[str] = []

        self.walletListener.start()
        self.topuptipbot.start(
            ("37b47fcaeb067582eb0b4230632633adffa7753481139b67cc8fe3ce", self.toHex("PERNIS")), 1000000)

        # (discordId, PERNIS count)

        self.current_prince: tuple[discord.Member, int, datetime, float] = None

        self.prince_game_duration = 86400

    @commands.command()
    async def puller(self, ctx):
        """Displays the statistics of the token faucet, including tokens remaining.

        Args:
            ctx (commands.Context): Discord context object
        """
        balance = requests.get("http://127.0.0.1:5000/tokbal").text

        returnEmbed = discord.Embed(
            title=f"{balance} $PERNIS left in the Pernis Puller")
        returnEmbed.add_field(
            name='Avg PERNIS per pull:',
            value=f"{int(round(0.000015*int(balance)))} $PERNIS",
        )

        await ctx.send(embed=returnEmbed)

    @commands.command()
    async def balance(self, ctx):
        """Allows a user to check the balance of their tipbot account.

        Args:
            ctx (commands.Context): Discord command context object
        """

        userID = ctx.author.id

        if not self.user_manager.user_exists(userID):
            await self.generate_new_user(userID)

        addr = self.user_manager.user_dict[userID].walletAddr
        lovelace_balance = self.user_manager.get_lovelace_balance(userID)
        asset_balances = self.user_manager.get_assets_balance(userID)

        pmEmbed = discord.Embed(
            title=f"PERNIS Tipbot Balance: {ctx.author.name}#{ctx.author.discriminator}")
        pmEmbed.set_image(url=ctx.author.avatar_url)
        pmEmbed.add_field(name='Ada Balance', value=str(
            round(Decimal(lovelace_balance/1000000), 6)))

        for asset_tuple, amount in asset_balances.items():
            pmEmbed.add_field(
                name=f'{self.fromHex(asset_tuple[1])} Balance', value=str(amount))

        await ctx.send(embed=pmEmbed)

    @commands.command()
    async def deposit(self, ctx):
        """Allows a user to view their tipbot deposit address.

        Args:
            ctx (commands.Context): Discord command context object.
        """

        userID = ctx.author.id

        if not self.user_manager.user_exists(userID):
            await self.generate_new_user(userID)

        addr = self.user_manager.user_dict[userID].walletAddr

        embedDesc = (
            "You can deposit ADA or any native asset into your PERNIS tipbot account by sending them to the address below. "
            + "\n\nThis is your unique address and it is permanently yours. Anything sent to this address will be credited to your account. "
            + "Note that this service is custodial, so all ADA and assets sent here are at your own risk."
            + "\n\nThe funds should be reflected in your account within a few minutes of transaction confirmation."
        )

        qr_image_path = await self.get_addr_qr_path(addr)
        qr_image_file = discord.File(qr_image_path, f"{addr}.png")

        pmEmbed = discord.Embed(title=f"PERNIS Tipbot Deposit: {ctx.author.name}#{ctx.author.discriminator}",
                                description=embedDesc)
        pmEmbed.set_image(url=f'attachment://{addr}.png')
        pmEmbed.add_field(name='Address', value=str(addr))

        await ctx.author.send(embed=pmEmbed, file=qr_image_file)
        if ctx.author.is_on_mobile():
            await ctx.author.send("**Mobile friendly details below:**")
            await ctx.author.send(f"**{str(addr)}**")

    @commands.command()
    async def tiprandom(self, ctx, constraints: commands.Greedy[typing.Union[discord.Role, discord.Member]], amount: int, assetname: str, *remainingargs):
        """Allows a user to tip a random user within the specified constraints (eg, a user or a role) a specified amount of a native token.

        Args:
            ctx (commands.Context): Discord command context object.
            constraints (commands.Greedy[typing.Union[discord.Role, discord.Member]]): Roles or list of members from which to pick a member. Can be listed in sequence.
            amount (int): Number of tokens to tip.
            assetname (str): UTF name of asset to tip.
        """

        members_pool = []
        if constraints:
            for constraint in constraints:
                if isinstance(constraint, discord.Member):
                    if constraint not in members_pool:
                        members_pool.append(constraint)
                elif isinstance(constraint, discord.Role):
                    add_mems = [
                        member for member in constraint.members if member not in members_pool]
                    members_pool += add_mems
        else:
            members_pool = ctx.guild.members

        remaining_arguments = list(remainingargs)

        asset_arguments = [assetname]
        if remaining_arguments:
            asset_arguments.append(remaining_arguments[0])

        source_user_id = ctx.author.id
        dest_user = random.choice(members_pool)
        dest_user_id = dest_user.id
        await self.ensure_user_exists(source_user_id)
        await self.ensure_user_exists(dest_user_id)

        try:
            if amount < 1:
                raise MalformedArgumentsError(
                    "You must tip a nonzero quantity of the specified token.")

            asset_policy_id, asset_hex_str, used_policyID_bool = await self.resolve_assetname(source_user_id, asset_arguments)
            self.user_manager.transfer_tokens(
                source_user_id, dest_user_id, asset_policy_id, asset_hex_str, amount)

            message_str = self.generate_message_string(
                remaining_arguments, used_policyID_bool)

            await ctx.send(
                f"{ctx.author.mention} has tipped randomly selected user {dest_user.mention} {amount} {self.fromHex(asset_hex_str)}! {message_str}"
            )

        except InsufficientFundsError as e:
            await ctx.author.send(e)
        except MalformedArgumentsError as e:
            await ctx.author.send(e)

    @commands.command()
    async def tip(self, ctx, members: commands.Greedy[discord.Member], amount: int, assetname: str, *remainingargs):
        """Command to tip a series of mentioned users some quantity of a native asset, if the source user has sufficient balance.

        Args:
            ctx (commands.Context): Discord command context object.
            members (commands.Greedy[discord.Member]): Successive sequence of mentioned guild members.
            amount (int): Number of tokens to tip.
            assetname (str): Asset name of native asset to tip.

        """

        remaining_arguments = list(remainingargs)

        asset_arguments = [assetname]
        if remaining_arguments:
            asset_arguments.append(remaining_arguments[0])

        source_user_id = ctx.author.id
        dest_user_ids = [member.id for member in members]
        await self.ensure_user_exists(source_user_id)
        for member_id in dest_user_ids:
            await self.ensure_user_exists(member_id)

        try:
            if len(members) == 0:
                raise MalformedArgumentsError(
                    "You have not tagged a member to tip!")
            if amount < 1:
                raise MalformedArgumentsError(
                    "You must tip a nonzero quantity of the specified token.")

            asset_policy_id, asset_hex_str, used_policyID_bool = await self.resolve_assetname(source_user_id, asset_arguments)

            total_token_send_amount = len(members)*amount
            if self.user_manager.user_dict[source_user_id].get_asset_balance(asset_policy_id, asset_hex_str) < total_token_send_amount:
                raise InsufficientFundsError(
                    f"You do not have the sufficient token balance to send {total_token_send_amount} {asset_hex_str}. Actual balance: {self.user_manager.user_dict[source_user_id].get_asset_balance(asset_policy_id, asset_hex_str)}")

            mentionstr = ""
            for dest_user_id in dest_user_ids:
                self.user_manager.transfer_tokens(
                    source_user_id, dest_user_id, asset_policy_id, asset_hex_str, amount)
                mentionstr += f"<@{dest_user_id}> "

            message_str = self.generate_message_string(
                remaining_arguments, used_policyID_bool)

            if len(members) == 1:
                await ctx.send(
                    f"{ctx.author.mention} has tipped user {mentionstr}{amount} {self.fromHex(asset_hex_str)}! {message_str}"
                )

            else:
                await ctx.send(
                    f"{ctx.author.mention} has tipped users {mentionstr}{amount} {self.fromHex(asset_hex_str)} each! {message_str}"
                )

        except InsufficientFundsError as e:
            await ctx.author.send(e)
        except MalformedArgumentsError as e:
            await ctx.author.send(e)

    @commands.command()
    async def drip(self, ctx, pastseconds: int, amount: int, assetname: str, *remainingargs):
        """Tips the specified native tokens to all users who have sent a message in the past specified amount of seconds in the message context channel.

        Args:
            ctx (commands.Context): Discord command context object.
            pastseconds (int): Number of past seconds from which to fetch eligible users.
            amount (int): Number of TOTAL native assets to be split up and distributed amongst all eligible members.
            assetname (str): The UTF token name of the native asset to be distributed.

        """
        remaining_arguments = list(remainingargs)

        asset_arguments = [assetname]
        if remaining_arguments:
            asset_arguments.append(remaining_arguments[0])

        pastseconds_limited = min(86400, pastseconds)

        from_time = datetime.utcnow() - timedelta(seconds=pastseconds_limited)
        members_pool = []
        messages_eligible = await ctx.channel.history(limit=None, after=from_time).flatten()

        for message in messages_eligible:
            if message.author not in members_pool and message.author != ctx.author and not message.author.bot:
                members_pool.append(message.author)

        source_user_id = ctx.author.id
        dest_user_ids = [member.id for member in members_pool]
        await self.ensure_user_exists(source_user_id)
        for member_id in dest_user_ids:
            await self.ensure_user_exists(member_id)

        try:
            if not members_pool:
                raise MalformedArgumentsError(
                    "There are no eligible members in this time interval.")
            if amount < 1:
                raise MalformedArgumentsError(
                    "You must tip a nonzero quantity of the specified token.")
            asset_policy_id, asset_hex_str, used_policyID_bool = await self.resolve_assetname(source_user_id, asset_arguments)

            if self.user_manager.user_dict[source_user_id].get_asset_balance(asset_policy_id, asset_hex_str) < amount:
                raise InsufficientFundsError(
                    f"You do not have the sufficient token balance to send {amount} {asset_hex_str}. Actual balance: {self.user_manager.user_dict[source_user_id].get_asset_balance(asset_policy_id, asset_hex_str)}")

            yield_per_recipient = math.floor(amount/len(members_pool))
            if yield_per_recipient == 0:
                raise MalformedArgumentsError(
                    f"You have not specified a rain amount to ensure that each recipient gets at least 1 token. This could be because there are a lot of eligible recipients for time interval you specified: {len(members_pool)} members."
                )

            mentionstr = ""
            for dest_user_id in dest_user_ids:
                self.user_manager.transfer_tokens(
                    source_user_id, dest_user_id, asset_policy_id, asset_hex_str, yield_per_recipient)
                mentionstr += f"<@{dest_user_id}> "

            message_str = self.generate_message_string(
                remaining_arguments, used_policyID_bool)

            if len(members_pool) == 1:
                await ctx.send(f"{ctx.author.mention} has dripped user {mentionstr}{str(yield_per_recipient)} {self.fromHex(asset_hex_str)}! {message_str}")
            else:
                await ctx.send(f"{ctx.author.mention} has dripped users {mentionstr}{str(yield_per_recipient)} {self.fromHex(asset_hex_str)} each! {message_str}")

        except InsufficientFundsError as e:
            await ctx.author.send(e)
        except MalformedArgumentsError as e:
            await ctx.author.send(e)

    @commands.command()
    async def withdrawall(self, ctx, withdraw_addr: str):
        """Allows the user to withdraw all the native assets they have in their tipbot balance providing they have enough ADA in their balance to carry the UTXO.

        Args:
            ctx (commands.Context): Discord command context object.
            withdraw_addr (str): Bech32 Cardano address to withdraw to.
        """
        userID = ctx.author.id
        try:
            if withdraw_addr[0] == "$":
                handle_get = self.cgql_api.get_handle_addr(withdraw_addr)
                if not handle_get:
                    raise MalformedArgumentsError("The handle you have entered does not exist. Please try again.")
                withdraw_addr = handle_get

            lovelace_balance = self.user_manager.get_lovelace_balance(userID)
            assets_balance = self.user_manager.get_assets_balance(userID)
            nonzero_assets_balance = {
                asset_tuple: amount for asset_tuple, amount in assets_balance.items() if amount > 0}

            await self._withdraw(userID, withdraw_addr, lovelace_balance, nonzero_assets_balance, subtract_fee=True)
            await ctx.author.send("Your withdrawal has been submitted! Depending on network conditions, it may take a few minutes before your funds are reflected back in your wallet.")
        except InsufficientFundsError as e:
            await ctx.author.send(e)
        except MalformedArgumentsError as e:
            await ctx.author.send(e)

    @commands.command()
    async def redeem(self, ctx, redeemStr: str):
        """Allows the user to redeem all the accumulated MEE6 exp for PERNIS tokens.

        Args:
            ctx (commands.Context): Discord command context object.
            redeemStr (str): Amount of XP to redeem, or 'all' to redeem all.

        """
        userID = ctx.author.id
        totalXp, pastRedeemedXp = await self.getUserXpPair(userID)
        xpSavings = totalXp - pastRedeemedXp
        await self.ensure_user_exists(userID)

        try:
            if redeemStr == 'all':
                redeemAmount = xpSavings
            else:
                try:
                    redeemAmount = int(redeemStr)
                except:
                    raise MalformedArgumentsError(
                        "You have not entered a valid integer amount of XP to redeem. Type 'all' to redeem all, or enter the amount to redeem.")

            if redeemAmount < 100:
                raise MalformedArgumentsError(
                    "You must redeem at least 100 xp at a time.")
            if xpSavings < 100:
                raise InsufficientFundsError(
                    'You do not have enough xp to redeem!')
            if redeemAmount > xpSavings:
                raise InsufficientFundsError(
                    "You are trying to redeem more XP than you have in your balance.")

            infoEmbed = discord.Embed(
                title=f"Redeeming $PERNIS: {ctx.author.name}#{ctx.author.discriminator}")
            infoEmbed.set_image(url=ctx.author.avatar_url)
            infoEmbed.add_field(name='Xp Balance', value=str(xpSavings))
            infoEmbed.add_field(name='Redeeming Xp', value=str(redeemAmount))
            infoEmbed.add_field(name='Xp Balance Post-Redemption',
                                value=str(xpSavings-redeemAmount))

            await ctx.send(embed=infoEmbed)

            remtokens = requests.get("http://127.0.0.1:5000/tokbal").text
            numpulls = Decimal(str(round(redeemAmount/self.xpperpull, 4)))
            average_yield = int(
                round(Decimal('0.000015')*numpulls*int(remtokens)))

            client_appinfo = await self.client.application_info()
            ownbot_appid: int = client_appinfo.id

            self.user_manager.transfer_tokens(
                ownbot_appid, userID, "37b47fcaeb067582eb0b4230632633adffa7753481139b67cc8fe3ce", self.toHex("PERNIS"), average_yield)

            self.writeRedeemedXp(userID, pastRedeemedXp+redeemAmount)
            await ctx.send(
                f'{average_yield} PERNIS tokens have been added to the tipbot balance of <@{userID}>! Your new balance is {str(self.user_manager.user_dict[userID].get_asset_balance("37b47fcaeb067582eb0b4230632633adffa7753481139b67cc8fe3ce",self.toHex("PERNIS")))} PERNIS.'
            )

        except InsufficientFundsError as e:
            await ctx.author.send(e)
        except MalformedArgumentsError as e:
            await ctx.author.send(e)

    @commands.command()
    async def drawelementz(self, ctx, weighteddraws: int, plaindraws: int):
        xpdict = {member.id: await self.getUserXp(member.id) for member in ctx.guild.members}
        sumxp = sum(xpdict.values())
        cumrange = 0
        range_dict = {}
        for memberid, xp in xpdict.items():
            range_dict[range(cumrange, cumrange+xp)] = memberid
            cumrange += xp

        winner_ids = set()
        while len(winner_ids) < weighteddraws:
            random_range_number = random.randrange(0, sumxp)
            random_member_id = next(
                filter(lambda x: random_range_number in x[0], range_dict.items()))
            winner_ids.add(random_member_id)

        first_round_winners = winner_ids.copy()
        ctx.send(f"First round winners: {first_round_winners}")

        member_ids = list(xpdict.keys())
        while len(winner_ids) < weighteddraws+plaindraws:
            random_winner_id = random.choice(member_ids)
            winner_ids.add(random_winner_id)

        second_round_winners = winner_ids - first_round_winners
        ctx.send(f"Second round winners: {str(second_round_winners)}")

    # GIVE A ROLE TO THE CURRENT PERNIS PRINCE

    @commands.command()
    async def princecheck(self, ctx):
        ch = self.client.get_channel(950582771653410836)
        if self.current_prince is None:
            await ch.send(
                "The throne is currently empty! Pledge 100 PERNIS with 'p!princepledge 100' to become the PERNIS Prince..."
            )

        else:
            await ch.send(
                f"The current prince is {self.current_prince[0].name}#{self.current_prince[0].discriminator}, who has put forward {self.current_prince[1]} PERNIS! Their reign is due to end in {round((self.current_prince[2] - datetime.utcnow()).total_seconds())} seconds..."
            )

    @commands.command()
    async def princepledge(self, ctx, pledgeamount: int):
        ch = self.client.get_channel(950582771653410836)
        userID = ctx.author.id

        client_appinfo = await self.client.application_info()
        ownbot_appid: int = client_appinfo.id

        try:
            prince_role = ctx.guild.get_role(925611925755293766)

            if self.current_prince is not None:
                if userID == self.current_prince[0].id:
                    raise MalformedArgumentsError(
                        "You are the current prince! You cannot usurp yourself...")

                if self.user_manager.user_dict[userID].get_asset_balance(self.knownassets["PERNIS"][0], self.knownassets["PERNIS"][1]) < 100:
                    raise InsufficientFundsError(
                        "You do not have enough PERNIS to become the prince! You need at least 100 PERNIS to ascend to the throne.")
                if pledgeamount < 100:
                    raise InsufficientFundsError(
                        "You must pledge at least 100 PERNIS to ascend to the empty throne!")
                if pledgeamount > self.user_manager.user_dict[userID].get_asset_balance(self.knownassets["PERNIS"][0], self.knownassets["PERNIS"][1]):
                    raise InsufficientFundsError(
                        "You do not have the funds to make this pledge!")

                if not self.user_manager.user_dict[userID].get_asset_balance(self.knownassets["PERNIS"][0], self.knownassets["PERNIS"][1]) > self.current_prince[1]:
                    raise InsufficientFundsError(
                        "You do not have enough PERNIS to usurp the current prince!")
                if pledgeamount <= self.current_prince[1]:
                    raise InsufficientFundsError(
                        "You have not pledged enough of your funds to usurp the current price!")
                if pledgeamount > self.user_manager.user_dict[userID].get_asset_balance(self.knownassets["PERNIS"][0], self.knownassets["PERNIS"][1]):
                    raise InsufficientFundsError(
                        "You do not have the funds to make this pledge!")

                self.user_manager.transfer_tokens(
                    ownbot_appid, self.current_prince[0].id, self.knownassets["PERNIS"][0], self.knownassets["PERNIS"][1], self.current_prince[1])
                await self.current_prince[0].remove_roles(prince_role)
                self.writePrinceLeaderboard(self.current_prince[0].id, round(
                    self.current_prince[3]), round(time.time()), self.current_prince[1])

            self.user_manager.transfer_tokens(
                userID, ownbot_appid, self.knownassets["PERNIS"][0], self.knownassets["PERNIS"][1], pledgeamount)
            if self.current_prince is not None:
                await ch.send(f"The prince is dead, long live the prince! <@{userID}> has usurped {self.current_prince[0].mention} to become the new prince! The value of the throne is {pledgeamount} PERNIS.")
            else:
                await ch.send(f"Arise, ye! <@{userID}> has gained control of the throne and is now the current Pernis Prince! The value of the throne is {pledgeamount} PERNIS.")
            await ctx.author.add_roles(prince_role)
            self.current_prince = (
                ctx.author,
                pledgeamount,
                (
                    datetime.utcnow()
                    + timedelta(seconds=self.prince_game_duration)
                ),
                round(time.time()),
            )


            if not self.checkPrinceGame.is_running():
                self.checkPrinceGame.start()

        except InsufficientFundsError as e:
            await ctx.send(e)
        except MalformedArgumentsError as e:
            await ctx.send(e)

    @commands.command()
    async def princeleaderboard(self, ctx):
        lb = self.readPrinceLeaderboard()

        lbEmbed = discord.Embed(title="PERNIS Prince Leaderboard")
        if self.current_prince:
            lbEmbed.add_field(
                name='Current Prince', value=f"{self.current_prince[0].name}#{self.current_prince[0].discriminator}")
        else:
            lbEmbed.add_field(name="Current Prince",
                              value="Throne is currently vacant!")

        lbstring = functools.reduce(lambda prevString, lb_dict: prevString+((prince_member := self.client.get_user(lb_dict["id"])).name + "#" + prince_member.discriminator)+"\t"+str(
            lb_dict["reignstart"])+"\t"+str(lb_dict["reignend"])+"\t"+str(lb_dict["pledge"])+"\n", lb, "User\tReignStart\tReignEnd\tPledge\n")

        lbEmbed.add_field(name='Leaderboard', value=lbstring)

        await ctx.send(embed=lbEmbed)

    @tasks.loop(seconds=15)
    async def checkPrinceGame(self):
        if self.current_prince is not None and self.current_prince[
            2
        ] < datetime.utcnow():
            prince_role = self.client.get_guild(
                920580089723383818).get_role(925611925755293766)
            ch = self.client.get_channel(950582771653410836)
            # end the game
            await self.current_prince[0].remove_roles(prince_role)
            await ch.send(f"The ancient curse has struck and the current prince <@{self.current_prince[0].id}> has died. The throne is now empty!")
            self.writePrinceLeaderboard(self.current_prince[0].id, round(
                self.current_prince[3]), round(time.time()), self.current_prince[1])
            self.current_prince = None

            self.checkPrinceGame.stop()

    @tasks.loop(seconds=60.0)
    async def walletListener(self):
        """
        Loop that checks the wallet every 60 seconds for new deposits.

        Also checks if there are pending outgoings, and sends them out.
        """

        txs_processed_set = set(self.txs_processed)
        all_txs: list[cardanotransaction.Transaction] = self.wallet.transactions()

        new_txs = [tx for tx in all_txs if tx.txid not in txs_processed_set]
        new_incoming_txs = [tx for tx in new_txs if tx.local_inputs == []]

        for tx in new_incoming_txs:
            for output in tx.local_outputs:
                recipient_did = self.user_manager.get_addresses_dict()[
                    str(output.address)]

                self.user_manager.user_dict[recipient_did].credit_user_lovelace(
                    int(output.amount*1000000))
                for asset in output.assets:
                    asset_policy_id = asset[0].policy_id
                    asset_hex_name = asset[0].asset_name
                    amount = asset[1]

                    self.user_manager.user_dict[recipient_did].credit_user_token(
                        asset_policy_id, asset_hex_name, amount)

        self.txs_processed += [tx.txid for tx in new_incoming_txs]

        # send pending outgoings
        print(len(self.withdraw_pool))

        if self.withdraw_pool:
            msgMetadataString = "PERNIS Tipbot Withdrawal"
            outgoing_tx = self.wallet.transfer_multiple(self.withdraw_pool,
                                                        metadata={674: {"msg": [
                                                            msgMetadataString[i:i+64] for i in range(0, len(msgMetadataString), 64)]}},
                                                        passphrase="qwertyuiop")

            self.txs_processed.append(outgoing_tx.txid)
            self.withdraw_pool = []
        self.user_manager.save_dict_state()
        self.save_txs_processed()

    @tasks.loop(seconds=86400.0)
    async def topuptipbot(self, token_tuple: tuple[str, str], maintain_amount: int):
        client_appinfo = await self.client.application_info()
        ownbot_appid: int = client_appinfo.id

        own_pernisbalance = self.user_manager.user_dict[ownbot_appid].get_asset_balance(
            token_tuple[0], token_tuple[1])
        print(f"Before topup PERNIS balance: {str(own_pernisbalance)}")
        if own_pernisbalance < maintain_amount:
            print("Topping up PERNIS balance...")
            deficit = maintain_amount - own_pernisbalance

            currentSessions = self.readTopupSessions()
            if self.user_manager.user_dict[ownbot_appid].walletAddr not in currentSessions:
                self.writeTopupSession(
                    self.user_manager.user_dict[ownbot_appid].walletAddr, deficit)

                receivedPERNIS = False
                await asyncio.sleep(30)
                counter = 0

                while not receivedPERNIS and counter < 241:
                    await asyncio.sleep(30)

                    new_pernisbalance = self.user_manager.user_dict[ownbot_appid].get_asset_balance(
                        token_tuple[0], token_tuple[1])
                    if new_pernisbalance > own_pernisbalance:
                        receivedPERNIS = True
                    else:
                        counter += 1

                print("PERNIS topped up.")

                currentSessions = self.readTopupSessions()
                del currentSessions[self.user_manager.user_dict[ownbot_appid].walletAddr]
                self._writeTopupSessions(currentSessions)
            else:
                print("Top up session currently active. Cannot top up at the moment.")

        print(
            f"PERNIS Balance: {str(self.user_manager.user_dict[ownbot_appid].get_asset_balance(token_tuple[0], token_tuple[1]))}")

    def readPrinceLeaderboard(self):
        if "prince.json" not in os.listdir('./__config'):
            return []
        with open("./__config/prince.json", 'r') as f:
            return json.load(f)

    def writePrinceLeaderboard(self, prince_member_id, start_of_reign, end_of_reign, pernisamount):
        current_prince_leaderboard = self.readPrinceLeaderboard()
        current_prince_leaderboard.append({"id": prince_member_id, "reignstart": round(
            start_of_reign), "reignend": round(end_of_reign), "pledge": pernisamount})
        with open("./__config/prince.json", 'w') as f:
            json.dump(current_prince_leaderboard, f)

    #format is {walletAddr: numpulls}
    def writeTopupSession(self, walletAddr: str, numpulls: int):
        sessionsDict = self.readTopupSessions()
        sessionsDict[walletAddr] = numpulls
        self._writeTopupSessions(sessionsDict)

    def readTopupSessions(self) -> dict[str, int]:
        if "sessions.json" not in os.listdir('./__config'):
            return {}
        with open("./__config/sessions.json", 'r') as f:
            return json.load(f)

    def _writeTopupSessions(self, sessionsDict: dict[str, int]):
        with open("./__config/sessions.json", 'w') as f:
            json.dump(sessionsDict, f)

    def generate_message_string(self, args_list: list[str], used_pid_bool: bool):
        message_str = ""
        start_index = 1 if used_pid_bool else 0
        message_list = args_list[start_index:]
        if len(message_list) > 0:
            for elem in args_list[start_index:]:
                message_str += f'{str(elem)} '

        return f"```{message_str}```" if message_str else ""

    async def _withdraw(self, userID: int, withdraw_addr: str, withdraw_lovelace: int, withdraw_assets: dict[tuple[str, str], int], subtract_fee: bool):
        """
        Processes a withdrawal request, and adds the withdrawal to a pending pool to be processed by the listener.
        """

        prepared_destination, total_cost = await self._balance_tx_and_estimate_fee(userID, withdraw_addr, withdraw_lovelace, withdraw_assets, subtract_fee)
        self.withdraw_pool.append(prepared_destination)

        self.user_manager.user_dict[userID].deduct_user_lovelace(total_cost)

        if len(prepared_destination) == 3:
            for asset_pair in prepared_destination[2]:
                asset_policy_id = asset_pair[0].policy_id
                asset_hexname = asset_pair[0].asset_name
                amount = asset_pair[1]

                self.user_manager.user_dict[userID].deduct_user_token(
                    asset_policy_id, asset_hexname, amount)

        self.user_manager.save_dict_state()

    async def _balance_tx_and_estimate_fee(self, userID: int, withdrawal_addr: str, withdrawal_lovelace: int, withdrawal_assets: dict[tuple[str, str], int], subtract_fee: bool):
        """
        Checks if the user has sufficient funds to make the withdrawal. If not raises an InsufficientFundsError.

        Otherwise, it balances the transaction and returns the tuple (user_gets_lovelace, tx_fee)
        """
        if withdrawal_lovelace < self.calculate_min_bundled_lovelace(withdrawal_assets):
            raise MalformedArgumentsError(
                "You have not added enough ADA in your withdrawal to cover the minimum ADA bundle for a UTXO.")

        assets_dest = [(AssetID(assets_tuple[1], assets_tuple[0]), amount)
                       for assets_tuple, amount in withdrawal_assets.items()]
        if assets_dest:
            destinations = [(withdrawal_addr, Decimal(
                withdrawal_lovelace/1000000), assets_dest)]
        else:
            destinations = [
                (withdrawal_addr, Decimal(withdrawal_lovelace/1000000))]

        estimated_fee = self.estimate_fee_endpoint(destinations)

        total_cost = withdrawal_lovelace
        if not subtract_fee:
            total_cost += estimated_fee

        user_gets_lovelace = withdrawal_lovelace
        if subtract_fee:
            user_gets_lovelace -= estimated_fee

        if total_cost > self.user_manager.get_lovelace_balance(userID) or user_gets_lovelace < self.calculate_min_bundled_lovelace(withdrawal_assets):
            raise InsufficientFundsError(
                "You do not have enough funds in your balance to make a valid withdrawal. You must have at least enough to cover the minimum bundled ADA, plus the network fee.")

        if assets_dest:
            return (withdrawal_addr, Decimal(user_gets_lovelace/1000000), assets_dest), total_cost
        else:
            return (withdrawal_addr, Decimal(user_gets_lovelace/1000000)), total_cost

    def estimate_fee_endpoint(self, destinations) -> int:
        """
        Returns the estimated lovelace tx fee.
        """

        try:
            avgFeeTuple: tuple[Decimal, Decimal] = self.wallet.estimate_fee(
                destinations)
            return int(avgFeeTuple[0] * 1000000)

        except BadRequest as e:
            raise MalformedArgumentsError(
                "\n⛔ A withdrawal address is likely malformed. Please try again.") from e
        except CannotCoverFee as e:
            raise InsufficientFundsError(
                "\n⛔ You likely do not have enough funds to complete the transaction. Please adjust your values and try again."
            ) from e

        except RESTServerError as e:
            raise InsufficientFundsError(
                "\n⛔ You likely do not have enough funds to complete the transaction. Please adjust your values and try again."
            ) from e

    def calculate_min_bundled_lovelace(self, assets_value: dict[tuple[str, str], int]):
        if not assets_value:
            return 1

        numAssets = len(assets_value)
        sumAssetNameLengths = sum(
            len(assetIDObj[1])/2 for assetIDObj in assets_value)
        numPolicyIDs = len(
            list(dict.fromkeys([assetIDObj[0] for assetIDObj in assets_value]))
        )

        return 34482 * \
            (27+6+math.ceil((12*numAssets+sumAssetNameLengths+28*numPolicyIDs)/8))

    async def ensure_user_exists(self, userID: str):
        if not self.user_manager.user_exists(userID):
            await self.generate_new_user(userID)

    async def resolve_assetname(self, source_user_id: str, asset_arguments: list):
        if not asset_arguments:
            raise MalformedArgumentsError(
                "Syntax of command cannot be recognised. Please ensure that you enter an asset name.")

        used_policyID = False
        passed_asset_name: str = asset_arguments[0]

        if passed_asset_name.upper() in self.knownassets:
            asset_policy_id = self.knownassets[passed_asset_name.upper(
            )][0]
            asset_hex_str = self.knownassets[passed_asset_name.upper()][1]
        else:
            source_user_assets = self.user_manager.get_assets_balance(
                source_user_id)
            source_user_assets_names = [
                asset_tuple[1] for asset_tuple, amount in source_user_assets.items()]

            if self.toHex(passed_asset_name) not in source_user_assets_names:
                raise InsufficientFundsError(
                    "You do not seem to have the requested token within your balance. Note that token names are case sensitive. If the asset name contains a space, please enclose the entire asset name in double quotation marks.")

            if source_user_assets_names.count(self.toHex(passed_asset_name)) == 1:
                found_asset_tuple = [asset_tuple for asset_tuple, amount in source_user_assets.items(
                ) if asset_tuple[1] == self.toHex(passed_asset_name)][0]
                asset_policy_id = found_asset_tuple[0]
                asset_hex_str = found_asset_tuple[1]

            else:
                if len(asset_arguments) == 1:
                    raise MalformedArgumentsError(
                        "You have multiple tokens with the same token name within your balance. Please try again and specify the policy ID as an argument.")

                # multiple same name tokens - use policyid to resolve ambiguity
                asset_hex_str = self.toHex(passed_asset_name)
                asset_policy_id = asset_arguments[1]

                source_user_asset_tuples = [
                    (asset_tuple[0], asset_tuple[1]) for asset_tuple, amount in source_user_assets.items()]
                if (asset_policy_id, asset_hex_str) not in source_user_asset_tuples:
                    raise InsufficientFundsError(
                        "You do not seem to have the requested token within your balance. Please ensure you pass a valid policy ID.")
                used_policyID = True
        return asset_policy_id, asset_hex_str, used_policyID

    async def generate_new_user(self, userID):
        addr = await self.tipping_backend.generate_unassigned_address()
        self.user_manager.add_new_user(int(userID), addr)

    async def get_addr_qr_path(self, addr: str):
        if not os.path.isfile(f'./__qr/{addr}.png'):
            img = qrcode.make(addr)
            img.save(f'./__qr/{addr}.png')

        return f'./__qr/{addr}.png'

    def fromHex(self, hexStr: str) -> str:
        return bytearray.fromhex(hexStr).decode()

    def toHex(self, utfStr: str) -> str:
        return utfStr.encode("utf-8").hex()

    def save_txs_processed(self):
        with open("./__config/txs_processed.json", 'w') as f:
            json.dump(self.txs_processed, f)

    def readLog(self):
        if "botlog.json" not in os.listdir('./__config'):
            return {}
        with open("./__config/botlog.json", 'r') as f:
            return json.load(f)

    def writeLog(self, logDict):
        with open("./__config/botlog.json", 'w') as f:
            json.dump(logDict, f)

    @commands.command()
    async def info(self, ctx):
        userID = ctx.author.id
        totalXp, redeemedXp = await self.getUserXpPair(userID)
        xpSavings = totalXp - redeemedXp

        remtokens = requests.get("http://127.0.0.1:5000/tokbal").text
        numpulls = Decimal(str(round(xpSavings/self.xpperpull, 4)))
        average_yield = int(round(Decimal('0.000015')*numpulls*int(remtokens)))

        infoEmbed = discord.Embed(
            title=f"PERNIS Bank: {ctx.author.name}#{ctx.author.discriminator}")
        infoEmbed.set_image(url=ctx.author.avatar_url)
        infoEmbed.add_field(name='Total Accumulated Xp', value=str(totalXp))
        infoEmbed.add_field(name='Redeemed Xp', value=str(redeemedXp))
        infoEmbed.add_field(name='Xp Savings', value=str(xpSavings))
        infoEmbed.add_field(name='PERNIS Value', value=str(average_yield))

        await ctx.send(embed=infoEmbed)

    # forces rebuild of cache for latest info - so query chunks at a time for performance
    # input list of userIDs to save from having to rebuild cache every time

    async def getUserDetails(self, userIDList):
        await self.mee6_api.levels.get_all_leaderboard_pages()

        returndict = {}
        for user_id in userIDList:
            returndict[user_id] = await self.mee6_api.levels.get_user_details(user_id)

        return returndict

    #userID is int
    # logDict is of format
    #{userID (int): redeemedXp (int)}
    def readRedeemedXp(self, userID):
        logDict = self.readLog()

        if str(userID) in str(logDict):
            return logDict[str(userID)]
        self.writeRedeemedXp(userID, 0)
        return 0

    def writeRedeemedXp(self, userID, redeemedXp):
        logDict = self.readLog()
        logDict[str(userID)] = redeemedXp
        self.writeLog(logDict)

    async def getUserXpPair(self, userID):
        totalXp = await self.getUserXp(userID)
        redeemedXp = self.readRedeemedXp(userID)

        return totalXp, redeemedXp

    async def getUserLevel(self, userID):
        userDetailsDict = await self.getUserDetails([userID])
        return userDetailsDict[userID]['level']

    async def getUserXp(self, userID):
        userDetailsDict = await self.getUserDetails([userID])
        if (
            userID in userDetailsDict
            and userDetailsDict[userID] is None
            or userID not in userDetailsDict
        ):
            return 0
        else:
            return userDetailsDict[userID]['xp']


class UserManager():
    '''
    Manager for the tipping ecosystem. Keeps track of all users and their balances. Liaisons with local DB.

    Attributes:
        wallet (:obj:`Wallet): Wallet backend that is used to transact and get blockchain information.
        user_dict (:obj:`dict` of
                        'discordID (str)':
                            :obj:`User`
                        )
    '''

    def __init__(self, wallet: Wallet):
        self.wallet: Wallet = wallet

        if os.path.isfile('./__config/tip_state.json'):
            with open("./__config/tip_state.json", 'r') as f:
                serialised_dict: dict[int, dict[str,
                                                str | int | list]] = json.load(f)
            self.user_dict = self.deserialise_from_json(serialised_dict)
        else:
            self.user_dict: dict[int, User] = {}

        self._addresses_dict_cache = {}

    def user_exists(self, did: int) -> bool:
        return did in self.user_dict

    def add_new_user(self, did, assigned_addr):
        if self.user_exists(did):
            raise UserAlreadyExistsError(
                f"add_new_user is being called on a did {did} that already exists to the UserManager.")
        newUser = User(did, assigned_addr, 0, [])
        self.user_dict[did] = newUser
        self.save_dict_state()

    def is_valid_transfer(self, origin_Did: int, dest_Did: int, policy_id: str, assetname_hex: str, amount: int) -> bool:
        return self.user_dict[origin_Did].get_asset_balance(policy_id, assetname_hex) >= amount

    def transfer_tokens(self, origin_Did: int, dest_Did: int, policy_id: str, assetname_hex: str, amount: int):
        if not self.is_valid_transfer(origin_Did, dest_Did, policy_id, assetname_hex, amount):
            raise InsufficientFundsError(
                f"User {origin_Did} has insufficient funds to transfer asset of type [pid: `{policy_id}`, hexname: `{assetname_hex}`]. Actual balance: {self.user_dict[origin_Did].get_asset_balance(policy_id, assetname_hex)}")

        self.user_dict[origin_Did].deduct_user_token(
            policy_id, assetname_hex, amount)
        self.user_dict[dest_Did].credit_user_token(
            policy_id, assetname_hex, amount)
        self.save_dict_state()

    def get_lovelace_balance(self, did: int) -> int:
        return self.user_dict[did].get_lovelace_balance()

    def get_assets_balance(self, did: int) -> dict[tuple[str, str], int]:
        return self.user_dict[did].get_total_asset_balance()

    def save_dict_state(self):
        serialised_dict = self.serialise_to_json()
        with open("./__config/tip_state.json", 'w') as f:
            json.dump(serialised_dict, f)

    def serialise_to_json(self):
        return {discordId: userobj.serialise_to_json()
                for discordId, userobj in self.user_dict.items()}

    def deserialise_from_json(self, serialised_dict: dict[int, dict[str, str | int | list]]):
        temp_user_dict: dict[int, User] = {}
        for discordId, single_user_serialised in serialised_dict.items():
            newUser = User()
            newUser.deserialise_from_json(single_user_serialised)
            temp_user_dict[int(discordId)] = newUser

        return temp_user_dict

    def get_addresses_dict(self) -> dict[str, int]:
        """
        Returns a dictionary that maps the addresses to the discordIDs to which they are assigned.
        """
        if len(self._addresses_dict_cache) != len(self.user_dict):
            self._addresses_dict_cache = {
                user.walletAddr: did for did, user in self.user_dict.items()}

        return self._addresses_dict_cache


class User():
    """
    A user within the tipping ecosystem.

    Attributes:
        discordID (str): Discord ID of the user
        walletAddr (str): Bech32 payment address assigned to the user
        lovelaceBalance (int): Lovelace balance of the user
        assetBalance (:obj:`list` of :obj:`[[policyID (str), assetNameHex (str)], amount (int)]`): List of pairs (using lists for json serialisation)
            of assets and their balances
    """

    def __init__(self, discordID: int = None, walletAddr: str = None, lovelaceBalance: int = None, assetBalance: list = None):
        """
        Constructor. Can pass no args in order to return a new object to read a json.
        """
        self.discordID = discordID
        self.walletAddr = walletAddr
        self.lovelaceBalance = lovelaceBalance
        self.assetBalance = assetBalance

    def get_lovelace_balance(self) -> int:
        """
        Returns the lovelace balance of the user.
        """
        return self.lovelaceBalance

    def get_asset_balance(self, policy_id: str, asset_hex_name: str) -> int:
        """
        Returns the balance of the asset passed as an argument held by the user. Can return 0.
        """
        return next(
            (
                elem[1]
                for elem in self.assetBalance
                if elem[0] == [policy_id, asset_hex_name]
            ),
            0,
        )

    def get_total_asset_balance(self) -> dict[tuple[str, str], int]:
        if self.assetBalance is None:
            return {}

        return_dict = {}
        for elem in self.assetBalance:
            if elem[1] > 0:
                return_dict[(elem[0][0], elem[0][1])] = elem[1]

        return return_dict

    def has_asset(self, policy_id: str, asset_hex_name: str) -> bool:
        """
        Returns bool of whether the user has a positive balance of a specific asset.
        """
        return self.get_asset_balance(policy_id, asset_hex_name) > 0

    def credit_user_token(self, policy_id: str, asset_hex_name: str, amount: int):
        """
        Adds 'amount' of the specified token to the User's balance.
        """
        if not isinstance(amount, int):
            raise TypeError(
                f"Cannot credit a User {self.discordID} an amount of an asset of type [pid: `{policy_id}`, hexname: `{asset_hex_name}`] that is not type `int`.")
        if not self.has_asset(policy_id, asset_hex_name):
            self.assetBalance.append([[policy_id, asset_hex_name], amount])
        else:
            for elem in self.assetBalance:
                if elem[0] == [policy_id, asset_hex_name]:
                    elem[1] += amount

    def deduct_user_token(self, policy_id: str, asset_hex_name: str, amount: int):
        """
        Deducts 'amount' specified tokens from the user's balance. Raises InsufficientFundsError if there is insufficient balance to deduct.

        Does a check of asset balance first to ensure deduction can happen, then calls a negative self.credit_user_token.
        """
        if not isinstance(amount, int):
            raise TypeError(
                f"Cannot deduct a User {self.discordID} an amount of an asset of type [pid: `{policy_id}`, hexname: `{asset_hex_name}`] that is not type `int`.")
        if self.get_asset_balance(policy_id, asset_hex_name) < amount:
            raise InsufficientFundsError(
                f"Cannot deduct {amount} tokens of type [pid: `{policy_id}`, hexname: `{asset_hex_name}`] from User {self.discordID} - balance is insufficient. Actual balance is {str(self.get_asset_balance(policy_id, asset_hex_name))}."
            )

        self.credit_user_token(policy_id, asset_hex_name, amount*-1)
        if not self.has_asset(policy_id, asset_hex_name):
            rebuild_asset_balance = [
                elem for elem in self.assetBalance if elem[1] > 0]
            self.assetBalance = rebuild_asset_balance

    def credit_user_lovelace(self, amount: int):
        if not isinstance(amount, int):
            raise TypeError(
                f"Cannot credit a User {self.discordID} an amount of lovelace that is not type `int`.")
        self.lovelaceBalance += amount

    def deduct_user_lovelace(self, amount: int):
        if not isinstance(amount, int):
            raise TypeError(
                f"Cannot deduct a User {self.discordID} an amount of lovelace that is not type `int`.")
        if self.get_lovelace_balance() < amount:
            raise InsufficientFundsError(
                f"Cannot deduct {amount} lovelace from User {self.discordID} - balance is {str(self.get_asset_balance())}."
            )

        self.credit_user_lovelace(amount*-1)

    def deserialise_from_json(self, user_dict: dict):
        """
        Deserialises from json and sets the class attributes to the contents of the JSON. Has side effects and will overwrite previous values.

        user_dict (dict): Dictionary to deserialise of the form
                        {
                            'discordID': Discord ID of the user (str),
                            'wallet_address': Assigned wallet address of the user (str),
                            'lovelace_balance': Lovelace balance of the user (int),
                            'asset_balance': Asset balances of the user (:obj:`list` of :obj:`[[policyID (str), assetNameHex (str)], amount (int)]`)
                        }
        """

        if 'discordID' not in user_dict:
            raise KeyError(
                "Key 'discordID' not present in User deserialisation.")

        if isinstance(user_dict['discordID'], int):
            self.discordID = user_dict['discordID']
        else:
            raise TypeError(
                "Value of key 'discordID' is not type `str` in User deserialisation.")
        if 'wallet_address' not in user_dict:
            raise KeyError(
                "Key 'wallet_address' not present in User deserialisation.")

        if isinstance(user_dict['wallet_address'], str):
            self.walletAddr = user_dict['wallet_address']
        else:
            raise TypeError(
                "Value of key 'wallet_address' is not type `str` in User deserialisation.")
        if 'lovelace_balance' not in user_dict:
            raise KeyError(
                "Key 'lovelace_balance' not present in User deserialisation.")

        if isinstance(user_dict['lovelace_balance'], int):
            self.lovelaceBalance = user_dict['lovelace_balance']
        else:
            raise TypeError(
                "Value of key 'lovelace_balance' is not type `int` in User deserialisation.")
        if 'asset_balance' not in user_dict:
            raise KeyError(
                "Key 'asset_balance' not present in User deserialisation.")
        if not isinstance(user_dict['asset_balance'], list):
            raise TypeError(
                f"Value of key 'asset_balance' is not type `list` in User deserialisation: {str(user_dict['asset_balance'])}")
        for asset_balance_tuple in user_dict['asset_balance']:
            if len(asset_balance_tuple) != 2:
                raise TypeError(
                    f"Asset-balance tuple is malformed in User deserialisation: {str(asset_balance_tuple)}")
            if len(asset_balance_tuple[0]) != 2:
                raise TypeError(
                    f"Asset identifier is malformed in User deserialisation: {str(asset_balance_tuple)}")
            if not isinstance(asset_balance_tuple[1], int):
                raise TypeError(
                    f"Asset balance is malformed in User deserialisation: {str(asset_balance_tuple)}")

        self.assetBalance = user_dict['asset_balance']

    def serialise_to_json(self) -> dict[int, str | int | list]:
        if self.discordID is not None and self.walletAddr is not None and self.lovelaceBalance is not None and self.assetBalance is not None:
            return {
                'discordID': self.discordID,
                'wallet_address': self.walletAddr,
                'lovelace_balance': self.lovelaceBalance,
                'asset_balance': self.assetBalance
            }
        else:
            raise ValueError(
                "Cannot serialise a User instance with uninitialised (NoneType) class attributes.")


class InsufficientFundsError(Exception):
    pass


class UserAlreadyExistsError(Exception):
    pass


class MalformedArgumentsError(Exception):
    pass


class TippingBackend():

    def __init__(self, wallet: Wallet, user_manager: UserManager):
        self.wallet: Wallet = wallet
        self.untransacted_addrs: list = []
        self.user_manager: UserManager = user_manager

    # do one more check one level up, due to parallelism/asynchronous calls
    async def generate_unassigned_address(self) -> str:
        """
        Gets a random unassigned address. Note that address_pool_gap should be set to at least 100.
        """
        unassigned_addresses = self.get_unassigned_addresses()

        if len(unassigned_addresses) <= 50:
            await self.set_untransacted_addresses()
            unassigned_addresses = self.get_unassigned_addresses()

        # sanity check in case of non-guarantee from parallelism
        valid = False
        while not valid:
            index = random.randrange(0, len(unassigned_addresses))
            if unassigned_addresses[index] not in set(self.user_manager.get_addresses_dict().keys()):
                valid = True

        return unassigned_addresses[index]

    def get_unassigned_addresses(self) -> list[str]:
        """
        Gets a list of addresses from the instance variable and filters those for unassigned elements by accessing UserManager.
        """
        untransacted_addrs_set = set(self.untransacted_addrs)
        assigned_addrs_set = set(self.user_manager.get_addresses_dict().keys())

        return list(untransacted_addrs_set - assigned_addrs_set)

    async def set_untransacted_addresses(self):
        '''
        Retrieves a list of addresses that have not been involved in on-chain transactions from the Wallet backend and sets it to an instance variable.
        Note that this is NOT a guarantee of not being assigned to a user.
        '''
        all_addrs_tuples = self.wallet.addresses(with_usage=True)
        self.untransacted_addrs = [
            str(addr_tuple[0]) for addr_tuple in all_addrs_tuples if not addr_tuple[1]]


class CardanoGQL:
    def __init__(self, apiurl):
        self.apiurl = apiurl

    def __get_cardano_gql_query(self, querystr, variables=None):
        sendjson = {"query": querystr}
        hdr = {"Content-Type": "application/json"}

        if variables:
            sendjson["variables"] = variables

        req = requests.post(self.apiurl, headers=hdr, json=sendjson)

        return req.json()['data']

    @staticmethod
    def __fromHex(hexStr: str) -> str:
        return bytearray.fromhex(hexStr).decode()

    @staticmethod
    def __toHex(utfStr: str) -> str:
        return utfStr.encode("utf-8").hex()

    def addr_txs(self, payment_address, from_block):
        query = '''
                query addrTxs(
                    $address: String!
                    $fromBlock: Int!
                ) {
                    blocks (
                        where: { number : { _gte: $fromBlock}}
                        order_by: {number: asc}
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
        flat_req = [block['transactions'] for block in self.__get_cardano_gql_query(
            query, variables)['blocks'] if block['transactions']]
        return list(functools.reduce(operator.add, flat_req)) if flat_req else []

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

        variables = {"hashes": tx_hash_list}

        return self.__get_cardano_gql_query(query, variables)['transactions']

    def chain_tip(self):
        query = '''
        { cardano { tip { number slotNo epoch { number } } } }
        '''
        return self.__get_cardano_gql_query(query)

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

        return req['utxos'][0]['address'] if req['utxos'] else None


def setup(client):
    client.add_cog(Tip(client))
