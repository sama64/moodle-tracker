
import requests, base64

invoke_url = "https://integrate.api.nvidia.com/v1/chat/completions"
stream = True


headers = {
  "Authorization": "Bearer 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",
  "Accept": "text/event-stream" if stream else "application/json"
}

payload = {
  "model": "moonshotai/kimi-k2.5",
  "messages": [{"role":"user","content":""}],
  "max_tokens": 16384,
  "temperature": 1.00,
  "top_p": 1.00,
  "stream": stream,
  "chat_template_kwargs": {"thinking":True},
}



response = requests.post(invoke_url, headers=headers, json=payload)

if stream:
    for line in response.iter_lines():
        if line:
            print(line.decode("utf-8"))
else:
    print(response.json())
