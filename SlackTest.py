import requests

webhook_url = "https://hooks.slack.com/services/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"

payload = {
        "text": "안녕? 테스트야\n 테스트2"
}

requests.post(webhook_url, json=payload)