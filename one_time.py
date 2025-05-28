import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Авторизация
CREDENTIALS_DICT = {
    "type": "service_account",
    "project_id": "excelproject-455714",
    "private_key_id": "91bee9e13a81b39e2fed4a8d0247495d889005b4",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCSmZKio6dVZCD/\n3zMg0JipTmiCQY/VoJo4BAjLgTGMnJVCqB7w4sYBr+Qcb+XFQ77+RLVtFKeTnFG1\n5JDYNZvvIgWNg+vl6JiXeBrGjihpykMQqROXTttJ5Dk38VZ3PpMyEE6miivFDuZC\nB+8AcJUsIr15i3Vv0TaW13lF0R+2PVaRffFlKDS+NLkG5LKfsY8r+jbN8FiVbdLR\nmxzdT2OROewYxEJcvZn/4EywmMsLCrXb1tP7h35+nz5JPp0xVbZK6WM0G4G2WAgw\nl13WJ4cgcLek/gk7JiH50UdOEFRz6INsg+oMo3e6LKt/moOwjdhtmZ8gYW9pyKLm\nVhIeUs9tAgMBAAECggEAB19x4HwajlDa2AOBrsTO6LTozKQ/d578IpURXCrDMy8s\n0o1iAPtmue7+qe92vtUJZgBOn43KX8Ic2ekE39rSXNR5MwTPeGCaTAPeVN4RakCh\n3tSiB5oPrUetGQMqNhUPkhT/36BTUzXMnsWHX55J4k5c+R/qaBU9iZiMoNZDogag\nMW6G9qSoJeN4BvqNqR+ICOdTt9dCKHRxkByaUPTwksDsCWHo297U4BPcWsPjgP4Z\nJy4xbdTTOh4Ui7raB2KYd7ID2Js/WQ1fqq06VFU2mJo/2kCldL6f8d71OihPZL+4\nnqoCdg/1mju1kHKOxKye1DR336RGiMEasFnsNgUgAQKBgQDLhwkaqQ/5RO7xOuFE\nfbQ0acklAO7dhXeIomxV9UbdTn+Hct26YBLonSIM3/cYqu0AjJFpu1FrQmIuNBgS\nQcmETar4eiHdB/MhI/p6bB9CaMK2NAsXMnxb4hbTSHR99leLWuGoVE8PQRhhEIQ3\n23b5pyRTUQLFcCtirzOJv/JFQQKBgQC4ZUY0x56KMErpyBW17MVXnjPRPnu7tLsa\nm3ES5tkurD43HB5Hib1GmL6GwbM6sgLSuZMJoKhSsjH7b+px6Fp/ey/ZvwQ1vs3b\nPVHvuFvsp3VtNUKAhxp/OB/OCEkTnLP4Mb2CVfeKejhYZv/8+rqjD4XBjI/knQ9Q\nQfYP1FTjLQKBgQCEAP8spX5QxB7dory8eXNJk1r8fxBt6MTQf9gYIE9n9iPMq/mX\nifx5loChLRnMi//PnVwq4W07TgDzyqHaJYUYJG/BXSVdgGx2kClDAaF8pwmytyqC\nTyJNTeRUAOhdUksRfU5iqNvmHug6/EVlHRibb4al6yMK/2eER/H7Y900gQKBgQCL\nzu2uMvQ33mnOW5BqgX0W87JiIjf6mAuNHvJa3IEq7Bm3+y/SGdNS5Zj/33mfNT0C\nvQWJNTCqksVm2PIvL3b+VU5wkG4GuganBhVL5sJ76nQUO1+Sx90FPG6Q7qNJpXSm\n6D/BxKCNdCGolV/eVdSQscI+f+7R7Wug9II2ek1qeQKBgHg6ODKqZdrD+cxGJ9O7\ntmyrfrcrRJx+sIHJARNegvzilMmwqy85I0JSEfi3uYCsN/s4b5O5s1rSRAw50Di6\nDTa6zmCmBvgE5qVsB4KObzPNAkbkCRJ/BWbBRYs9Ta2x6JcZaM5reiGb+YFWQ7IL\nK/EGIF58hknnXCowedGKKGfR\n-----END PRIVATE KEY-----\n",
    "client_email": "drive-bot@excelproject-455714.iam.gserviceaccount.com",
    "client_id": "115902541194622230230",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/drive-bot%40excelproject-455714.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"
}

SPREADSHEET_ID = "1c3yRBMlcOmzK_g8FWCVD_rztGMXfjsuhF12YVVYNU94"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

creds = ServiceAccountCredentials.from_json_keyfile_dict(CREDENTIALS_DICT, SCOPES)
gc = gspread.authorize(creds)
ss = gc.open_by_key(SPREADSHEET_ID)

# Удаляем лист, если существует
sheet_name = "Sellers"
try:
    existing_ws = ss.worksheet(sheet_name)
    ss.del_worksheet(existing_ws)
except gspread.exceptions.WorksheetNotFound:
    pass

# Создаём лист и вставляем ссылки (без заголовков)
ws = ss.add_worksheet(title=sheet_name, rows="2", cols="1")
ws.update('A1:A2', [
    ["https://www.avito.ru/brands/rtdpremium/items/all/avtomobili?sellerId=3e48ca8ef29814e6b71446c333b66c2d&s=search_page_share"],
    ["https://auto.ru/reseller/3tyBCKbkMS4Uwo1iy0GVpaR1qCiB5CZo/all"]
])
