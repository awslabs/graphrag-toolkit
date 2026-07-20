# Copyright (c) Evan Erwee. All rights reserved.

"""Bedrock Enricher Plugin for enhancing documents with AI-generated content."""

import logging
import json
from typing import List, Dict, Any
from document_graph.transform.transformer_provider_base import TransformerProvider
from document_graph.document_graph_config import document_graph_config

logger = logging.getLogger(__name__)


class BedrockEnricherPlugin(TransformerProvider):
    """Enriches records using AWS Bedrock models for content analysis."""
    
    def __init__(self, config):
        super().__init__(config)
        
        self.task = self.args.get("task", "summarize")
        self.input_field = self.args.get("field", "content")
        self.model_id = self.args.get("model_id", "anthropic.claude-3-haiku-20240307-v1:0")
        self.prefix = self.args.get("prefix", "bedrock_")
        self.suffix = self.args.get("suffix", "")
        self.max_tokens = self.args.get("max_tokens", 200)
        self.temperature = self.args.get("temperature", 0.3)
        self.enable_dedup = self.args.get("enable_dedup", True)
        self.cache_dir = self.args.get("cache_dir", None)
        self._permission_error = False  # Circuit breaker for permission errors
        self._cache = {}  # In-memory cache for deduplication
        
        # Setup persistent cache if cache_dir is provided
        if self.cache_dir and self.enable_dedup:
            import os
            os.makedirs(self.cache_dir, exist_ok=True)
            self._load_cache_from_disk()
        
        # If suffix is provided, use field name as prefix
        if self.suffix:
            self.prefix = self.args.get('csv_property_name', self.input_field)
        
        self.client = document_graph_config._get_or_create_client("bedrock-runtime")

    def _load_prompt_template(self, task: str) -> str:
        """Load prompt template with fallback to document graph prompts."""
        import os
        
        # Try enricher-specific prompts first
        enricher_prompt_path = os.path.join(
            os.path.dirname(__file__), "prompts", f"evidence_{task}.txt"
        )
        
        if os.path.exists(enricher_prompt_path):
            with open(enricher_prompt_path, 'r') as f:
                return f.read().strip()
        
        # Fallback to document graph prompts
        try:
            from document_graph.prompts import get_prompt
            return get_prompt(f"evidence_{task}")
        except:
            pass
        
        # Default fallback
        if task == "summarize":
            return "Summarize the key evidence from this JSON data in 2-3 sentences:\n\n{text}"
        elif task == "tag":
            return "Extract key tags from this evidence data:\n\n{text}"
        else:
            return "Analyze this evidence data:\n\n{text}"
    
    def _make_prompt(self, text: str) -> str:
        template = self._load_prompt_template(self.task)
        return template.format(text=text)

    def transform(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        enriched_records = []
        
        # Skip all processing if we've hit a permission error
        if self._permission_error:
            logger.warning("Skipping Bedrock enrichment due to previous permission error")
            return records
        
        for record in records:
            enriched_record = record.copy()
            text = record.get(self.input_field, "")
            
            if text and isinstance(text, str) and text.strip():
                field_name = f"{self.prefix}{self.suffix}" if self.suffix else f"{self.prefix}{self.task}"
                
                # Check cache for deduplication
                if self.enable_dedup:
                    import hashlib
                    text_hash = hashlib.md5(text.encode()).hexdigest()
                    if text_hash in self._cache:
                        logger.debug(f"Using cached result for duplicate evidence (hash: {text_hash[:8]}...)")
                        enriched_record[field_name] = self._cache[text_hash]
                        enriched_records.append(enriched_record)
                        continue
                
                try:
                    prompt = self._make_prompt(text)
                    
                    # Different request formats for different models
                    if "anthropic.claude" in self.model_id:
                        body = json.dumps({
                            "anthropic_version": "bedrock-2023-05-31",
                            "max_tokens": self.max_tokens,
                            "temperature": self.temperature,
                            "messages": [{"role": "user", "content": prompt}]
                        })
                    elif "amazon.nova" in self.model_id:
                        body = json.dumps({
                            "messages": [{"role": "user", "content": [{"text": prompt}]}],
                            "inferenceConfig": {
                                "maxTokens": self.max_tokens,
                                "temperature": self.temperature
                            }
                        })
                    else:
                        # Default format
                        body = json.dumps({
                            "inputText": prompt,
                            "textGenerationConfig": {
                                "maxTokenCount": self.max_tokens,
                                "temperature": self.temperature
                            }
                        })
                    
                    response = self.client.invoke_model(
                        modelId=self.model_id,
                        body=body
                    )
                    
                    response_body = json.loads(response['body'].read())
                    
                    # Different response formats for different models
                    if "anthropic.claude" in self.model_id:
                        output = response_body['content'][0]['text'].strip()
                    elif "amazon.nova" in self.model_id:
                        output = response_body['output']['message']['content'][0]['text'].strip()
                    else:
                        output = response_body.get('results', [{}])[0].get('outputText', '').strip()
                    
                    enriched_record[field_name] = output
                    
                    # Cache the result for deduplication
                    if self.enable_dedup:
                        self._cache[text_hash] = output
                        logger.info(f"Cached new result for evidence hash: {text_hash[:8]}...")
                        # Save to disk if cache_dir is configured
                        if self.cache_dir:
                            self._save_cache_to_disk()
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # Check for permission errors and set circuit breaker
                    if "AccessDeniedException" in error_msg or "bedrock:InvokeModel" in error_msg:
                        self._permission_error = True
                        logger.error(f"Bedrock permission error - stopping enrichment: {error_msg}")
                        error_field = f"{self.prefix}{self.suffix}_error" if self.suffix else f"{self.prefix}{self.task}_error"
                        enriched_record[error_field] = "Permission denied for Bedrock access"
                        enriched_records.append(enriched_record)
                        break  # Stop processing immediately
                    else:
                        logger.warning(f"Bedrock enrichment failed: {error_msg}")
                        error_field = f"{self.prefix}{self.suffix}_error" if self.suffix else f"{self.prefix}{self.task}_error"
                        enriched_record[error_field] = error_msg
            
            enriched_records.append(enriched_record)
        
        return enriched_records
    
    def _load_cache_from_disk(self):
        """Load cache from disk if it exists."""
        import os
        import json
        
        cache_file = os.path.join(self.cache_dir, f"bedrock_cache_{self.task}.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    self._cache = json.load(f)
                logger.debug(f"Loaded {len(self._cache)} cached results from {cache_file}")
            except Exception as e:
                logger.warning(f"Failed to load cache from {cache_file}: {e}")
    
    def _save_cache_to_disk(self):
        """Save cache to disk."""
        if not self.cache_dir:
            return
            
        import os
        import json
        
        cache_file = os.path.join(self.cache_dir, f"bedrock_cache_{self.task}.json")
        try:
            with open(cache_file, 'w') as f:
                json.dump(self._cache, f, indent=2)
            logger.debug(f"Saved {len(self._cache)} cached results to {cache_file}")
        except Exception as e:
            logger.warning(f"Failed to save cache to {cache_file}: {e}")