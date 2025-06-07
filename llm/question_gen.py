from dags.lib.PineconeManager import PineconeManager
import os
from dotenv import load_dotenv
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch

load_dotenv()

model_name = "TinyLlama/TinyLlama-1.1B-Chat-v1.0"

tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    torch_dtype=torch.float16
).to("cuda" if torch.cuda.is_available() else "cpu")

pinecone_key = os.getenv("PINECONE_API")
index_name = "letstalkvector"
namespace = "letstalk-ns"

pi = PineconeManager(index_name, namespace, pinecone_key)
def generate_question(interests):
    questions = []
    for interest in interests:
        response = pi.query(interest, num_results=1)
        result = response.get("result").get('hits')[0].get('fields').get('text_to_embed')

        input_text = f"""Generate exactly ONE question about the following topic. Do NOT answer it. Make it FUN and INFORMAL. Do not refer directly to what I'm passing you as the user can't see it.
        Topic: {result}
        Question:"""

        inputs = tokenizer(input_text, return_tensors="pt").to(model.device)

        outputs = model.generate(
            **inputs,
            max_new_tokens=40,
            do_sample=True,
            temperature=0.7,
            top_k=40,
            top_p=0.9,
            num_return_sequences=3,
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.eos_token_id,
            repetition_penalty=1.2
        )

        for i, out in enumerate(outputs):
            full_text = tokenizer.decode(out, skip_special_tokens=True)
            question = full_text.split("Question:")[-1].split("\n")[0].split("?")[0].strip()
            questions.append(question)


    return questions