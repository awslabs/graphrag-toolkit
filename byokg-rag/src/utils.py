import yaml
import os.path as osp
from colorama import Fore, Style
import re
import json
import string



def load_graph(graph_path):
    try:
        with open(graph_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Graph file not found: {graph_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in graph file: {graph_path}")
    

def load_yaml(file_path):
    file_path =  file_path if file_path.startswith('/') else osp.join(osp.dirname(osp.abspath(__file__)), file_path)
    with open(file_path, 'r') as file:
        content = yaml.safe_load(file)
    return content

def color_print(text, color):
    print(getattr(Fore, color.upper()) + Style.BRIGHT + text + Style.RESET_ALL)

def parse_response(response, pattern):

    if not isinstance(response, str):
        return []

    match = re.search(pattern, response, flags=re.DOTALL)
    matched = []
    if match:
        graph_text = match.group(1)
        for to_match in graph_text.strip().split('\n'):
            if to_match != "":
                matched.append(to_match)

    return matched


def normalize(s: str) -> str:
    """Lower text and remove punctuation, articles and extra whitespace."""
    s = s.lower()
    exclude = set(string.punctuation)
    s = "".join(char for char in s if char not in exclude)
    s = re.sub(r"\b(a|an|the)\b", " ", s)
    # remove <pad> token:
    s = re.sub(r"\b(<pad>)\b", " ", s)
    s = " ".join(s.split())
    return s


def match(s1: str, s2: str) -> bool:
    s1 = normalize(s1)
    s2 = normalize(s2)
    return s2 in s1