import os

api_key = os.environ.get("OPENAI_API_KEY")

if api_key:
    print("✅ OPENAI_API_KEY is set.")
    print("First 8 characters:", api_key[:8], "...")
else:
    print("❌ OPENAI_API_KEY is NOT set. Check Environment Variables.")
