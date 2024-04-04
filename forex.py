import asyncio
import aiohttp
import aiofiles
import json
import time
from datetime import datetime
import pandas as pd
import keys

class Forex:
    def __init__(self):
        self.APP_CODE = keys.ALI_APP_CODE # showapi api coder
        self.cache_alive = 60 * 60 * 1  # 1 hour
        self.required_currencies = ['USD', 'HKD', 'JPY', 'GBP', 'EUR', 'AUD', 'CAD', 'SGD']
        self.bank_codes = ["ICBC", "BOC", "ABCHINA", "BANKCOMM", "CCB", "CMBCHINA", "CEBBANK", "SPDB", "CIB", "CIB_HYRS", "ECITIC", "HSBC"]
        self.bank_names = {
            "ICBC": "工商银行",
            "BOC": "中国银行",
            "ABCHINA": "农业银行",
            "BANKCOMM": "交通银行",
            "CCB": "建设银行",
            "CMBCHINA": "招商银行",
            "CEBBANK": "光大银行",
            "SPDB": "浦发银行",
            "CIB": "兴业银行",
            "CIB_HYRS": "兴业银行（寰宇优惠）",
            "ECITIC": "中信银行",
            "HSBC": "汇丰银行"
        }
        self.last_update = None
    
    async def reload_rates(self):
        await self.get_all_rates(use_cache=False)

    async def cache_rates(self, bank_code, rates):
        async with aiofiles.open(f"forex_cache/{bank_code}.json", "w") as f:
            await f.write(json.dumps({
                "timestamp": time.time(),
                "rates": rates
            }))

    async def get_cached_rates(self, bank_code):
        try:
            async with aiofiles.open(f"forex_cache/{bank_code}.json", "r") as f:
                data = json.loads(await f.read())
                if time.time() - data["timestamp"] < self.cache_alive:
                    return data["rates"]
        except:
            pass
        return None

    async def ask_ali(self, bank_code):
        url = "https://ali-waihui.showapi.com/bank10"
        params = {
            "bankCode": bank_code
        }
        headers = {
            "Authorization": f"APPCODE {self.APP_CODE}"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers, verify_ssl=False) as response:
                if response.status == 200:
                    result = (await response.json())["showapi_res_body"]["codeList"]
                    result = self._format_rates(result)
                    await self.cache_rates(bank_code, result)
                    return result
                else:
                    raise Exception(f"Failed to get forex from {bank_code}")

    async def get_cib_hyrs_rates(self):
        try:
            res = await self.ask_ali("CIB")
            df = pd.DataFrame(res)

            discounted_df = pd.DataFrame({
                'exchangeRateCurrency': df['exchangeRateCurrency'],
                'transferBuyingRate': ((3*pd.to_numeric(df['transferBuyingRate'], errors="coerce") + pd.to_numeric(df['transferSellingRate'], errors="coerce"))/4).round(4),
                'transferSellingRate': ((pd.to_numeric(df['transferBuyingRate'], errors="coerce") + 3*pd.to_numeric(df['transferSellingRate'], errors="coerce"))/4).round(4),
                'notesBuyingRate': ((3*pd.to_numeric(df['notesBuyingRate'], errors="coerce") + pd.to_numeric(df['notesSellingRate'], errors="coerce"))/4).round(4),
                'notesSellingRate': ((pd.to_numeric(df['notesBuyingRate'], errors="coerce") + 3*pd.to_numeric(df['notesSellingRate'], errors="coerce"))/4).round(4),
            })

            discounted_df = discounted_df.sort_values(by='exchangeRateCurrency')
            discounted_df = discounted_df.reset_index(drop=True)
            res = discounted_df.to_dict(orient="records")
            await self.cache_rates("CIB_HYRS", res)
            return res
        except Exception as e:
            print(f"Error fetching CIB_HYRS rates: {str(e)}")
            return self._empty_rates_dict()

    async def get_hsbc_rates(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("https://www.services.cn-banking.hsbc.com.cn/mobile/channel/digital-proxy/cnyTransfer/ratesInfo/remittanceRate",
                                    headers={'Content-Type': 'application/json'}, params={'locale': 'en_CN'}) as response:
                    res = (await response.json(content_type=None))["data"]["counterForRepeatingBlock"]

            df = pd.DataFrame(res)
            df_filtered = df[df['exchangeRateCurrency'].isin(self.required_currencies)]
            formatted_df = pd.DataFrame({
                'exchangeRateCurrency': df_filtered['exchangeRateCurrency'],
                'transferBuyingRate': (1/pd.to_numeric(df_filtered['transferBuyingRate'], errors="coerce")).round(4),
                'transferSellingRate': (1/pd.to_numeric(df_filtered['transferSellingRate'], errors="coerce")).round(4),
                'notesBuyingRate': (1/pd.to_numeric(df_filtered['notesBuyingRate'], errors="coerce")).round(4),
                'notesSellingRate': (1/pd.to_numeric(df_filtered['notesSellingRate'], errors="coerce")).round(4),
            })

            formatted_df = formatted_df.sort_values(by='exchangeRateCurrency')
            formatted_df = formatted_df.reset_index(drop=True)
            res = formatted_df.to_dict(orient="records")
            await self.cache_rates("HSBC", res)
            return res
        except Exception as e:
            print(f"Error fetching HSBC rates: {str(e)}")
            return self._empty_rates_dict()

    async def get_rates(self, bank_code, use_cache=True):
        assert bank_code in self.bank_codes
        if use_cache:
            rates = await self.get_cached_rates(bank_code)
            if rates:
                return rates

        try:
            self.last_update = datetime.now().strftime("%Y-%m-%d %H:%M")
            if bank_code == "HSBC":
                return await self.get_hsbc_rates()

            if bank_code == "CIB_HYRS":
                return await self.get_cib_hyrs_rates()

            return await self.ask_ali(bank_code)
        except Exception as e:
            print(f"Error fetching rates for {bank_code}: {str(e)}")
            return self._empty_rates_dict()

    async def get_all_rates(self, use_cache=True):
        tasks = []
        for bank_code in self.bank_codes:
            tasks.append(asyncio.create_task(self.get_rates(bank_code, use_cache)))
        results = await asyncio.gather(*tasks)
        return dict(zip(self.bank_codes, results))
    
    async def get_currency_rates(self, currency, use_cache=True):
        assert currency in self.required_currencies
        tasks = []
        for bank_code in self.bank_codes:
            tasks.append(asyncio.create_task(self.get_rates(bank_code, use_cache)))
        results = await asyncio.gather(*tasks)
        
        currency_rates = []
        for bank_code, rates in zip(self.bank_codes, results):
            if rates:
                for rate in rates:
                    if rate['exchangeRateCurrency'] == currency:
                        currency_rates.append({
                            'Bank': bank_code,
                            'TransferBuyingRate': rate['transferBuyingRate'],
                            'TransferSellingRate': rate['transferSellingRate'],
                            # 'NotesBuyingRate': rate['notesBuyingRate'],
                            # 'NotesSellingRate': rate['notesSellingRate']
                        })
                        break
            else:
                currency_rates.append({
                    'Bank': bank_code,
                    'TransferBuyingRate': None,
                    'TransferSellingRate': None
                })

        return currency_rates, self.last_update

    def _format_rates(self, rates):
        df = pd.DataFrame(rates)
        df_filtered = df[df['code'].isin(self.required_currencies)]
        formatted_df = pd.DataFrame({
            'exchangeRateCurrency': df_filtered['code'],
            'transferBuyingRate': (pd.to_numeric(df_filtered['hui_in'], errors="coerce")/100).round(4),
            'transferSellingRate': (pd.to_numeric(df_filtered['hui_out'], errors="coerce")/100).round(4),
            'notesBuyingRate': (pd.to_numeric(df_filtered['chao_in'], errors="coerce")/100).round(4),
            'notesSellingRate':( pd.to_numeric(df_filtered['chao_out'], errors="coerce")/100).round(4)
        })
        formatted_df = formatted_df.sort_values(by='exchangeRateCurrency')
        formatted_df = formatted_df.reset_index(drop=True)
        return formatted_df.to_dict(orient="records")
    
    def _empty_rates_dict(self):
        return [{
            'exchangeRateCurrency': currency,
            'transferBuyingRate': None,
            'transferSellingRate': None,
            'notesBuyingRate': None,
            'notesSellingRate': None
        } for currency in self.required_currencies]
    


if __name__ == "__main__":
    forex = Forex()
    print(pd.DataFrame(asyncio.run(forex.get_currency_rates("USD", use_cache=False))))
    