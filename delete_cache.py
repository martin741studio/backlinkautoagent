import os

cache_file = 'data/module_2_cache.json'
if os.path.exists(cache_file):
    os.remove(cache_file)
    print("✅ Cache deleted successfully!")
else:
    print("Cache file does not exist.")
