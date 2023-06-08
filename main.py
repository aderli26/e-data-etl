import pandas as pd
import json

DATA_URL = 'data/cur.zip'
JSON_URL = 'data/fix.json'

df = pd.read_csv(DATA_URL)
with open(JSON_URL, "r") as f:
    fix_json = json.loads(f.read())
    f.close()

# filter the product name & ItemType
for replace_json in fix_json:
    df_replace = df[(df['product/ProductName'] == "Amazon CloudFront")
                    & (df['lineItem/LineItemType'] == "Usage")]

    # filter the UsageType

    df_replace = df_replace[df_replace['lineItem/UsageType']
                            == replace_json['lineItem/UsageType']]
    # filter the UsageAccountId
    df_replace = df_replace[df_replace['lineItem/UsageAccountId'].astype(
        str) == replace_json['lineItem/UsageAccountId']]

    if len(df_replace):
        df_replace = df_replace[['lineItem/UsageAccountId', 'product/ProductName', 'lineItem/LineItemType', 'lineItem/UsageType', 'lineItem/LineItemDescription',
                                'lineItem/UsageAmount', 'lineItem/UnblendedRate', 'lineItem/UnblendedCost']]
        # replace description
        df_replace['lineItem/LineItemDescription'] = df_replace['lineItem/LineItemDescription'].str.replace(
            df_replace['lineItem/UnblendedRate'].astype(str).values[0], str(replace_json['lineItem/UnblendedRate']))

        # replace UnblendedRate
        df_replace['lineItem/UnblendedRate'] = replace_json['lineItem/UnblendedRate']

        # calculate new cost
        df_replace['lineItem/UnblendedCost'] = df_replace['lineItem/UsageAmount'] * \
            df_replace['lineItem/UnblendedRate']

        # output to zip
        df_replace.to_csv("output/"+str(replace_json['lineItem/UsageAccountId'])+".zip",
                          index=False,
                          compression="zip")
