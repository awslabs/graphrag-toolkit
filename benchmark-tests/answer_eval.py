from bedrock_generator import BedrockGenerator
import argparse
import json
import multiprocessing as mp

PROMPT = """You are an evaluator tasked with determining whether a specific statement is semantically entailed in a response. 
Given the question: {question}: 
Check whether the following statement is entailed/covered in the response. Response Yes/No without any other text.
Statement: {evidence}
Response: {response}
"""

def run_evaluation(prompt):
    response = generator.generate(text=[
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
    ])
    print(f"prompt: {prompt}")
    print(f"entailment: {response}")
    return response

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-file-path", required=True, type=str)
    parser.add_argument("--output-file-path", type=str)
    parser.add_argument("--query-file-path", type=str) # only needed for multihop-rag
    parser.add_argument("--dataset", type=str, choices = ["multihop-rag", "wikihowqa"])
    args = parser.parse_args()
    generator = BedrockGenerator.from_config({"model_id": "us.anthropic.claude-3-sonnet-20240229-v1:0"})
    with open(args.input_file_path) as fin:
            data = json.load(fin)
    arguments = []
    entries = []
    cnt = 0

    if args.dataset == "multihop-rag":
        with open(args.query_file_path) as fin:
            queries = json.load(fin)
        for query, response in zip(queries, data):
            for evidence in query[0]["evidence_list"]:
                prompt = PROMPT.format(evidence=evidence["fact"], response=response["response"])
                arguments.append(prompt)

    elif args.dataset == "wikihowqa":
        for entry in data:
            evidences = entry["answers"][0].split(".")
            for evidence in evidences:
                if len(evidence) ==0:
                    continue
                prompt = PROMPT.format(question = entry["raw_question"], evidence=evidence, response=entry["response"])
                arguments.append(prompt)
                entries.append({"question": entry["raw_question"], "evidence": evidence, "response": entry["response"]})
    else:
        raise NotImplemented
    
    pool = mp.Pool(16)
    responses = pool.map(run_evaluation, arguments)
    correct = 0
    total = 0
    for idx, response in enumerate(responses):
        if "Yes" in response:
            correct += 1
            entries[idx]["judgement"] = "Yes"
        else:
            entries[idx]["judgement"] = "No"
        total += 1
    print(correct, total)
    print(correct / total)

    with open(args.output_file_path, 'w') as output_file:
        json.dump(entries, output_file, indent=2)
