current_timestamp=$(date +%s000)

if [ -d "cache" ]; then
    mv "cache" "cache-$current_timestamp"
fi   

if [ -f "cache.zip" ]; then
    mv "cache.zip" "cache-$current_timestamp.zip"
fi

python regen_cache.py

zip -r cache.zip cache