from sklearn.feature_extraction.text import TfidfVectorizer

from sklearn.metrics.pairwise import cosine_similarity

news_1 = """Introducing Bubblemaps (BMT) on Binance HODLer Airdrops! Earn BMT With Retroactive BNB Simple Earn Subscriptions
https://www.binance.com/en/support/announcement/detail/6539265e571540298fa4b7943140469d"""

news_2 = """Binance has announced the 12th project on the HODLer Airdrops page - Bubblemaps (BMT)
Users who subscribed their BNB to Simple Earn (Flexible and/or Locked) and/or On-Chain Yields products from 2025-03-02 00:00 UTC to 2025-03-06 23:59 UTC will get the airdrops distribution.
Binance will then list BMT at 2025-03-18 15:00 UTC and open trading against USDT, USDC, BNB, FDUSD, and TRY pairs.
HODLer Airdrops Token Rewards: 30,000,000 BMT (3% of max token supply)
https://www.binance.com/en/support/announcement/detail/6539265e571540298fa4b7943140469d"""


vectorizer = TfidfVectorizer()
tfidf_matrix = vectorizer.fit_transform([news_1, news_2])

cosine_sim = cosine_similarity(tfidf_matrix[0], tfidf_matrix[1])

print(f"Cosine Similarity Score: {cosine_sim[0][0]:.4f}")
