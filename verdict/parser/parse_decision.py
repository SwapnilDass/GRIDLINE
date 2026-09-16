import pdfplumber
import re
import os
import glob


LABEL_MAP = {
    "No / Driver": "Driver",
    "Competitor": "Competitor",
    "Time": "Time",
    "Session": "Session",
    "Fact": "Fact",
    "Infringement": "Infringement",
    "Offence": "Infringement",
    "Decision": "Decision",
    "Reason": "Reason",
}


def get_lines(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = pdf.pages[0].extract_text()
    return text.split("\n")

def extract_own_car(driver_text):
    match = re.search(r"^(\d+)", driver_text)
    return int(match.group(1)) if match else None

def extract_other_car(fact_text, reason_text, own_car):
    for text in (fact_text, reason_text):
        cars = [int(n) for n in re.findall(r"[Cc]ar\s+(\d+)", text)]
        others = [c for c in cars if c != own_car]
        if others:
            return others[0]
    return None

def extract_penalty_points(decision_text):
    match = re.search(r"(\d+)\s+[Pp]enalty [Pp]oints", decision_text)
    return int(match.group(1)) if match else None

def extract_turn(fact_text):
        match = re.search(r"[Tt]urn\s+(\d+)", fact_text)
        return int(match.group(1)) if match else None


def parse_decision(pdf_path):
    lines = get_lines(pdf_path)
    result = {}
    current_label = None
    buffer = []

    for line in lines:
        matched_label = None
        for label in LABEL_MAP:
            if line.startswith(label) and (len(line) == len(label) or not line[len(label)].islower()):

                matched_label = label
                break

        if matched_label:
            if current_label:
                result[current_label] = " ".join(buffer).strip()
            current_label = LABEL_MAP[matched_label]
            buffer =  [line[len(matched_label):].strip()]
        elif current_label:
            buffer.append(line.strip())

    if current_label:
        result[current_label] = " ".join(buffer).strip()

    result["Reason"] = result["Reason"].split("Competitors are reminded")[0].strip()

    result["own_car"] = extract_own_car(result["Driver"])

    result["other_car"] = extract_other_car(result["Fact"], result["Reason"], result["own_car"])

    result["penalty_points"] = extract_penalty_points(result["Decision"])


    result["turn"] = extract_turn(result["Fact"])

    return result

if __name__ == "__main__":
    pdf_folder = "verdict/data/raw"
    pdf_files = glob.glob(os.path.join(pdf_folder, "*.pdf"))

    for pdf_path in pdf_files:
        print(f"=== {pdf_path} ===")
        try:
            data = parse_decision(pdf_path)
            for key, value in data.items():
                print(f"{key}: {value}")
        except Exception as e:
            print(f"FAILED: {e}")
        print()
