import requests
from datetime import datetime, timedelta

# Global variable to store the token and expiration time
token_info = dict(token = None , expires = datetime.now())

def request_token():
    """
    Request a new authentication token and store its expiration true.
    """
    global token_info

    url = "https://delivery-financial-backend.mottu.cloud/api/v2/Usuario/AutenticarPorEmail"

    payload = dict(email = "bruno.alves@mottu.com.br", senha = "mottu.4034")
    headers = {"Content-Type": "application/json"}

    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        # Assuming the response includes a token and an expiry period or token itself has expiry info
        token_data = response.json()
        token_info["token"] = token_data["dataResult"]["token"]
        # Set expiration time to 6 day from now
        token_info["expires"] = datetime.now() + timedelta(days=6)
        return token_data["dataResult"]["token"]
    else:
        raise Exception("Failed to authenticate and obtain token")
    

def get_token():
    """
    Retrieve the current token if it's valid, or request a new one if it's expired.
    """
    global token_info
    if datetime.now() >= token_info["expires"]:
        agora = datetime.now().strftime('%A, %B %d, %Y %I:%M:%S %p')
        expires = token_info["expires"].strftime('%A, %B %d, %Y %I:%M:%S %p')
        return request_token(), f"Expires: {expires}" , agora # Request new token if the current one is expired
    return token_info["token"]  # Return the existing token if it's still valid

def make_authenticated__post_request(url, data):
    """
    Make an authenticated request using the bearer token.
    """
    
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response  = requests.post(url, json=data, headers=headers)
    
    if response.ok:
        return response.json() # Return the JSON response if successful
    
    return response.raise_for_status()  # Raise an error for bad responses