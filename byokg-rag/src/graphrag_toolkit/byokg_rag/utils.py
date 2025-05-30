import yaml
import os.path as osp
from colorama import Fore, Style
import re
import string


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
