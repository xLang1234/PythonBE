import asyncio
from twikit import Client

USERNAME = 'xx'
EMAIL = 'xx'
PASSWORD = 'xx'

# Initialize client
client = Client('en-US')

async def main():
    a = await client.login(
        auth_info_1=USERNAME,
        auth_info_2=EMAIL,
        password=PASSWORD,
        cookies_file='cookies.json'
    )
    
    
    tweets = await client.get_user_tweets((await client.get_user_by_screen_name('bgarlinghouse')).id, 'Tweets')
    for tweet in tweets:
        print(
            tweet.user.name,
            tweet.user.id,
            tweet.text,
            tweet.created_at
        )

asyncio.run(main())