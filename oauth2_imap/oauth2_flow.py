"""Process Oauth2 authentication flow."""

import sys
import json
import logging
import msal
import os, atexit

from oauth2_imap.config import Config

class Oauth2Flow:
    """Process Oauth2 authentication flow."""
    def __init__(self, conf: Config) -> None:
        """Initialize the Oauth2 Flow."""
        self.conf = conf
        self.cache = msal.SerializableTokenCache()
        if os.path.exists(self.conf.TOKEN_CACHE_FILE):
            self.cache.deserialize(open(self.conf.TOKEN_CACHE_FILE, "r").read())
            print("token cache loaded")

        atexit.register(
            lambda: open(self.conf.TOKEN_CACHE_FILE, "w").write(self.cache.serialize())
            # Hint: The following optional line persists only when state changed
            if self.cache.has_state_changed
            else None
        )

    def get_access_token(self) -> "tuple[str, str]":
        """Create a preferably long-lived app instance which maintains a token cache."""
        app = msal.PublicClientApplication(
            self.conf.AZURE_CLIENT_ID,
            authority=self.conf.AZURE_AUTHORITY,
            token_cache=self.cache
            # token_cache=...  # Default cache is in memory only.
            # You can learn how to use SerializableTokenCache from
            # https://msal-python.rtfd.io/en/latest/#msal.SerializableTokenCache
        )

        result = None
        # Try to reload token from the cache
        accounts = app.get_accounts()
        if accounts:
            result = app.acquire_token_silent(
                scopes=self.conf.AZURE_SCOPE,
                account=accounts[0],
                authority=None, 
                force_refresh=False,
                claims_challenge=None,
            )

        if not result:
            logging.info("No suitable token exists in cache. Let's get a new one from AAD.")

            flow = app.initiate_device_flow(scopes=self.conf.AZURE_SCOPE)
            if "user_code" not in flow:
                raise ValueError("Fail to create device flow. Err: %s" % json.dumps(flow, indent=4))

            print(flow["message"])
            sys.stdout.flush()  # Some terminal needs this to ensure the message is shown

            # Ideally you should wait here, in order to save some unnecessary polling
            # input("Press Enter after signing in from another device to proceed, CTRL+C to abort.")

            result = app.acquire_token_by_device_flow(flow)  # By default it will block
            # You can follow this instruction to shorten the block time
            #    https://msal-python.readthedocs.io/en/latest/#msal.PublicClientApplication.acquire_token_by_device_flow
            # or you may even turn off the blocking behavior,
            # and then keep calling acquire_token_by_device_flow(flow) in your own customized loop.

        if "access_token" in result:
            # return the access token AND the username
            if not accounts:
                accounts = app.get_accounts()
            print("Token aquired for:", accounts[0]["username"])
            if 'scope' in result:
                print('result["scope"]', result["scope"])
            return result["access_token"], accounts[0]["username"]
        else:
            raise ValueError(
                "Error getting access_token",
                result.get("error"),
                result.get("error_description"),
                result.get("correlation_id"),
            )
