import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(usecwd=True), override=True)

# dump to shell export format
for k, v in os.environ.items():
    print(f'export {k}="{v}"')