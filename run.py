from passlib.hash import sha256_crypt
import requests

crypt_pass = sha256_crypt.encrypt("super secret password")
# ans = requests.get('http://localhost:5001/registered_functions', headers={'Authorization': crypt_pass}).json()
# print(ans)
ans = requests.get('http://172.24.0.4:5001/registered_functions', headers={'Authorization': crypt_pass})
print(crypt_pass)
print(ans.json())


ans = requests.get('http://localhost:5002/actual_service_ip', headers={'Authorization': crypt_pass}).json()
print(ans)

ans = requests.get('http://localhost:5004/node_states', headers={'Authorization': crypt_pass}).json()
print(ans)

