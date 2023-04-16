from cryptography.fernet import Fernet

fernet_key = Fernet.generate_key()

# 你的fernet_key，把它放在安全的地方
print(fernet_key.decode())

