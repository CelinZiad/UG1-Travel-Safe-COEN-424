from functools import lru_cache

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from .config import get_settings


class LocalAIError(RuntimeError):
    pass


@lru_cache
def _load_local_model():
    """
    Load the DistilGPT-2 model and tokenizer once (process-wide).
    """
    settings = get_settings()
    model_name = settings.hf_local_model

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    # Workaround: distilgpt2 has no pad token; use eos_token
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(model_name)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    model.eval()

    return tokenizer, model, device


def generate_text_with_local_model(
    prompt: str,
    max_new_tokens: int = 160,
    temperature: float = 0.7,
) -> str:
    """
    Use the local DistilGPT-2 model to generate text completion for the given prompt.
    """
    tokenizer, model, device = _load_local_model()

    inputs = tokenizer(prompt, return_tensors="pt").to(device)

    with torch.no_grad():
        output_ids = model.generate(
            **inputs,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            do_sample=True,
            top_p=0.9,
            pad_token_id=tokenizer.eos_token_id,
        )

    full_text = tokenizer.decode(output_ids[0], skip_special_tokens=True)

    # The model outputs prompt + completion; try to strip the prompt prefix if present
    if full_text.startswith(prompt):
        completion = full_text[len(prompt):].strip()
        return completion or full_text.strip()

    return full_text.strip()
