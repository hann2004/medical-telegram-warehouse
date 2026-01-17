
# --- Imports ---
from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from typing import List
from api.database import get_db
from api.schemas import TopProduct, ChannelActivity, MessageSearchResult, VisualContentStats, TrendingChannel

# --- FastAPI App ---
app = FastAPI(
	title="Medical Telegram Warehouse Analytical API",
	description="REST API for analytical queries on the medical telegram data warehouse."
)

# --- Endpoint 1: Top Products ---
@app.get(
	"/api/reports/top-products",
	response_model=List[TopProduct],
	summary="Top Products",
	description="Returns the most frequently mentioned terms/products across all channels."
)
def get_top_products(limit: int = Query(10, ge=1, le=100, description="Number of top products to return (max 100)"), db: Session = Depends(get_db)):
	try:
		# List of common English stopwords to filter out
		stopwords = [
			'the','and','for','with','are','from','that','this','was','but','not','all','you','your','has','have','had','his','her','she','him','our','out','who','how','why','can','will','would','could','should','may','might','shall','do','did','does','of','on','in','to','by','as','at','an','or','if','is','it','be','been','so','we','they','their','them','a','i','he','my','me','no','yes','up','down','over','under','about','into','than','then','too','very','just','more','most','some','such','only','own','same','other','each','any','both','few','which','what','when','where','why','while','again','further','once','here','there','after','before','because','during','between','through','above','below','off','against','under','until','upon','these','those','doing','being','having','also','per','via','etc','etc.'
		]
		stopwords_sql = ",".join([f"'{w}'" for w in stopwords])
		sql = text(f'''
			SELECT lower(word) as term, count(*) as count
			FROM (
				SELECT unnest(string_to_array(regexp_replace(message_text, '[^a-zA-Z0-9 ]', '', 'g'), ' ')) as word
				FROM raw.fct_messages
				WHERE message_text IS NOT NULL
			) words
			WHERE length(word) > 2
			  AND lower(word) NOT IN ({stopwords_sql})
			GROUP BY lower(word)
			ORDER BY count DESC
			LIMIT :limit
		''')
		result = db.execute(sql, {"limit": limit})
		products = [TopProduct(term=row[0], count=row[1]) for row in result]
		if not products:
			raise HTTPException(status_code=404, detail="No products found.")
		return products
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# --- Endpoint 2: Channel Activity ---
@app.get(
	"/api/channels/{channel_name}/activity",
	response_model=List[ChannelActivity],
	summary="Channel Activity",
	description="Returns posting activity and trends for a specific channel."
)
def get_channel_activity(channel_name: str, db: Session = Depends(get_db)):
	try:
		sql = text('''
			SELECT to_char(d.full_date, 'YYYY-MM-DD') as date, count(m.message_id) as message_count
			FROM raw.fct_messages m
			JOIN raw.dim_channels c ON m.channel_key = c.channel_key
			JOIN raw.dim_dates d ON m.date_key = d.date_key
			WHERE c.channel_name = :channel_name
			GROUP BY d.full_date
			ORDER BY d.full_date
		''')
		result = db.execute(sql, {"channel_name": channel_name})
		activity = [ChannelActivity(date=row[0], message_count=row[1]) for row in result]
		if not activity:
			raise HTTPException(status_code=404, detail="Channel not found or no activity.")
		return activity
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# --- Endpoint 3: Message Search ---
@app.get(
	"/api/search/messages",
	response_model=List[MessageSearchResult],
	summary="Message Search",
	description="Searches for messages containing a specific keyword."
)
def search_messages(query: str = Query(..., description="Keyword to search for in messages"), limit: int = Query(20, ge=1, le=100, description="Number of messages to return (max 100)"), db: Session = Depends(get_db)):
	try:
		sql = text('''
			SELECT m.message_id, c.channel_name, m.message_text, d.full_date
			FROM raw.fct_messages m
			JOIN raw.dim_channels c ON m.channel_key = c.channel_key
			JOIN raw.dim_dates d ON m.date_key = d.date_key
			WHERE m.message_text ILIKE :pattern
			ORDER BY d.full_date DESC
			LIMIT :limit
		''')
		result = db.execute(sql, {"pattern": f"%{query}%", "limit": limit})
		messages = [MessageSearchResult(
			message_id=row[0],
			channel_name=row[1],
			message_text=row[2],
			message_date=row[3]
		) for row in result]
		if not messages:
			raise HTTPException(status_code=404, detail="No messages found for the given query.")
		return messages
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# --- Endpoint 4: Visual Content Stats ---
@app.get(
	"/api/reports/visual-content",
	response_model=List[VisualContentStats],
	summary="Visual Content Stats",
	description="Returns statistics about image usage across channels."
)
def get_visual_content_stats(db: Session = Depends(get_db)):
	try:
		sql = text('''
			SELECT c.channel_name,
				   COUNT(DISTINCT m.image_path) AS unique_images,
				   COUNT(m.image_path) AS total_images,
				   COUNT(DISTINCT m.message_id) AS messages_with_images
			FROM raw.fct_messages m
			JOIN raw.dim_channels c ON m.channel_key = c.channel_key
			WHERE m.image_path IS NOT NULL
			GROUP BY c.channel_name
			ORDER BY unique_images DESC
		''')
		result = db.execute(sql)
		stats = [VisualContentStats(
			channel_name=row[0],
			unique_images=row[1],
			total_images=row[2],
			messages_with_images=row[3]
		) for row in result]
		if not stats:
			raise HTTPException(status_code=404, detail="No visual content stats found.")
		return stats
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# --- Endpoint 5: Trending Channels ---
@app.get(
	"/api/reports/trending-channels",
	response_model=List[TrendingChannel],
	summary="Trending Channels",
	description="Returns channels with the highest increase in posting activity over the last 7 days compared to the previous 7 days."
)
def get_trending_channels(db: Session = Depends(get_db)):
	try:
		sql = text('''
			WITH recent AS (
				SELECT c.channel_name,
					   SUM(CASE WHEN d.full_date >= CURRENT_DATE - INTERVAL '7 days' THEN 1 ELSE 0 END) AS last_7_days,
					   SUM(CASE WHEN d.full_date >= CURRENT_DATE - INTERVAL '14 days' AND d.full_date < CURRENT_DATE - INTERVAL '7 days' THEN 1 ELSE 0 END) AS prev_7_days
				FROM raw.fct_messages m
				JOIN raw.dim_channels c ON m.channel_key = c.channel_key
				JOIN raw.dim_dates d ON m.date_key = d.date_key
				GROUP BY c.channel_name
			)
			SELECT channel_name, last_7_days, prev_7_days, (last_7_days - prev_7_days) AS increase
			FROM recent
			ORDER BY increase DESC, last_7_days DESC
			LIMIT 10
		''')
		result = db.execute(sql)
		trending = [TrendingChannel(
			channel_name=row[0],
			last_7_days=row[1],
			prev_7_days=row[2],
			increase=row[3]
		) for row in result]
		if not trending:
			raise HTTPException(status_code=404, detail="No trending channels found.")
		return trending
	except Exception as e:
		raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
