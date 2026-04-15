from boto3 import Session
from botocore.config import Config
import logging
import json
import argparse
from tqdm import tqdm
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def call_bedrock_invoke_model(prompt, bedrock, model_id, is_json_output=True):
    while True:
        try:
            accept = 'application/json'
            contentType = 'application/json'

            if 'anthropic.claude-3' in model_id:
                payload_body = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "max_tokens": 2048,
                    "temperature": 0.0,
                    "top_p": 1,
                    "top_k": 50,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": prompt
                                }
                            ]
                        }
                    ]
                }
                body = json.dumps(payload_body)
            else:
                body = json.dumps({"prompt": prompt, "max_tokens_to_sample": 2048, "temperature": 0.0, "top_p": 1, "top_k": 50,
                                "stop_sequences": ["\\n\\nHuman:"]})

            response = bedrock.invoke_model(body=body, modelId=model_id, accept=accept, contentType=contentType)

            if 'anthropic.claude-3' in model_id:
                response = response['body'].read().decode('utf-8')
                response = json.loads(response)
                response_text = response['content'][0]['text']

            else:
                response_obj = response.get('body').read().decode('utf-8')
                print("RESPONSE OBJ: " + str(response_obj))
                json_response_obj = json.loads(response_obj)
                response_text = json_response_obj['completion']
            if is_json_output:
                try:
                    start_idx = response_text.find("{")
                    end_idx = response_text.find("}")
                    parsed_completion = response_text[start_idx:end_idx + 1]
                    parsed_json = json.loads(parsed_completion)
                    parsed_json['llm_response'] = response_text
                    return parsed_json
                except:
                    logger.error(response_text)
                    return {
                        'grade': "incorrect",
                        'justification': "LLM failed grading",
                        'llm_response': response_text
                    }
            else:
                return response_text
        except Exception as e:
            logger.error(str(e))
            time.sleep(3)
        


BKB_CORRECTNESS_GRADING = """
Human:
You are a teacher grading a quiz. 
You are given a question, the student's answer, and the true answer, and are asked to score the student answer as either Correct or Incorrect.

Example Format:
QUESTION: question here
STUDENT ANSWER: student's answer here
TRUE ANSWER: true answer here
GRADE: Correct or Incorrect here

Grade the student answers based ONLY on their factual accuracy. Ignore differences in punctuation and phrasing between the student answer and true answer. It is OK if the student answer contains more information than the true answer, as long as it does not contain any conflicting statements. If the student fails to answers or claims that the search results do not mention the answer then mark as incorrect. Begin! 

QUESTION: {query}
STUDENT ANSWER: {answer}
TRUE ANSWER: {expected_answer}
GRADE:

Your response should be in json format as follows:
{{
    "grade": (correct or incorrect),
    "justification": (Without mentioning the student/teacher framing of this prompt, explain why the STUDENT ANSWER is Correct or Incorrect. Use one or two sentences maximum. Keep the answer as concise as possible.)
}}


Assistant:
"""

IDK_DETECTION = """You are a teacher grading a quiz. Based on students' response, you are asked to determine if the students think they can not answer the question because some information are missing.
Response: {response}
Please output "Unanswerable" if the students identify that they can not answer the question. Otherwise, output "Answerable".
"""
import os
os.environ["AWS_REGION_NAME"] = "us-west-2"
    

class GenerationEvaluator:
    bedrock = Session().client(
        service_name='bedrock-runtime',
        region_name="us-west-2",
        config=Config(
            max_pool_connections=50,
            retries={"max_attempts": 10, "mode": "standard"},
            connect_timeout=500,
            read_timeout=500,
            region_name="us-west-2"  
        ))
    

    def __init__(self, model_id):
        self.model_id = model_id

class CorrectnessEvaluator(GenerationEvaluator):
    def __init__(self, model_id):
        super().__init__(model_id)
    
    def evaluate(self, question, answer, response):
        grading = {}
        grading.update(self._llm_evaluate(question, answer, response))
        return grading
        
    def _llm_evaluate(self, question, answer, response):
        prompt = BKB_CORRECTNESS_GRADING.format(
            query=question,
            answer=response,
            expected_answer=answer
        )
        completion = call_bedrock_invoke_model(prompt, self.bedrock, model_id=self.model_id)
        if answer == "":
            completion['grade'] = "incorrect"
            completion['justification'] = "No answer was provided"

        if not completion or not completion['grade'] or not completion['justification']:
            logger.error("Failed to grade")
            logger.error(str(completion))
            return {
                'question': question,
                'llmCorrectnessGrade': "incorrect",
                'llmCorrectnessGradeJustification': "LLM failed grading",
                'llm_response': completion.get('llm_response', str(completion))  # Store the raw response
            }

        try:
            grading = {
                'question': question,
                'llmCorrectnessGrade': completion['grade'].lower(),
                'llmCorrectnessGradeJustification': completion['justification'].replace("\"", "\\\""),
                'llm_response': completion.get('llm_response', str(completion))  # Store the raw response
            }
            return grading
        except Exception as e:
            logger.info(str(e))
            return {
                'question': question,
                'llmCorrectnessGrade': "incorrect",
                'llmCorrectnessGradeJustification': "LLM failed grading",
                'llm_response': completion.get('llm_response', str(completion))  # Store the raw response
            }
            

class IDKEvaluator(GenerationEvaluator):
    def __init__(self, model_id):
        super().__init__(model_id)
    
    def evaluate(self, question, answer, response):
        grading = {}
        grading.update(self._llm_evaluate(question, answer, response))
        return grading
    
    def _llm_evaluate(self, question, answer, response):
        prompt = IDK_DETECTION.format(
            question=question,
            answer=answer,
            response=response
        )
        completion = call_bedrock_invoke_model(prompt, self.bedrock, model_id=self.model_id, is_json_output=False)
        if "Unanswerable" in completion:
            return {
                "label": "unanswerable"
            }
        else:
            return {
                "label": "answerable"
            }
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-file-path", type=str)
    parser.add_argument("--metrics-output-path", type=str)
    parser.add_argument("--eval-artifacts", type=str)
    parser.add_argument("--metric", type=str, default="correctness", choices=["correctness", "idk", "correctness_on_answerable"])
    args = parser.parse_args()

    if args.metric == "correctness_on_answerable":
        eval_correctness_artifact, eval_idk_artifact = args.eval_artifacts.split(",")
        with open(eval_correctness_artifact) as fin:
            eval_correctness_data = json.load(fin)
        with open(eval_idk_artifact) as fin:
            eval_idk_data = json.load(fin)
        assert len(eval_correctness_data) == len(eval_idk_data)
        total, count = 0, 0
        for correctness_eval, idk_eval in zip(eval_correctness_data, eval_idk_data):
            if idk_eval["label"] == "answerable":
                total += 1
                if correctness_eval["llmCorrectnessGrade"] == "correct":
                    count += 1
        logger.info("{}: {}".format(args.metric, count / total))
    else:
        if args.metric == "correctness":
            evaluator = CorrectnessEvaluator(model_id="anthropic.claude-3-sonnet-20240229-v1:0")
        elif args.metric == "idk":
            evaluator = IDKEvaluator(model_id="anthropic.claude-3-sonnet-20240229-v1:0")
        

        data = []
        with open(args.input_file_path) as fin:
            for line in fin:
                data.append(json.loads(line))
        evaluation_outputs = []
        for example in tqdm(data):
            answer = example["raw_example"]["answer"]
            response = example["response"]
            question = example["raw_example"]["question"]
            evaluation_outputs.append(evaluator.evaluate(question, answer, response))
        count, total = 0, 0
        for evaluation in evaluation_outputs:
            total += 1
            if args.metric == "correctness":
                if evaluation["llmCorrectnessGrade"] == "correct":
                    count += 1
            if args.metric == "idk":
                if evaluation["label"] == "unanswerable":
                    count += 1
        
        logger.info("{}: {}".format(args.metric, count / total))
        os.makedirs(os.path.dirname(args.metrics_output_path), exist_ok=True)
        with open(args.metrics_output_path, "w") as fout:
            json.dump(evaluation_outputs, fout, indent=4)
        with open(os.path.join(os.path.dirname(args.metrics_output_path), "{}.json".format(args.metric)), "w") as fout:
            json.dump({
                args.metric: count / total
            }, fout, indent=4)

import multiprocessing as mp
import yaml

class SafeCounter():
    # constructor
    def __init__(self):
        # initialize counter
        self._counter = mp.Value('i', 0)
        # initialize lock
        self._lock = mp.Lock()
 
    # increment the counter
    def increment(self):
        # get the lock
        with self._lock:
            self._counter.value += 1
 
    # get the counter value
    def value(self):
    	with self._lock:
        	return self._counter.value
         
def read_yaml(file_path):
    if file_path:
        with open(file_path, 'r') as file:
            data = yaml.safe_load(file)
        return data
    else:
        return {}