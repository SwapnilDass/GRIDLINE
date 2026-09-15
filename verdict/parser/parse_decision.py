import pdfplumber

LABELS = ["Fact", "Infringement", "Decision", "Reason"]


def get_lines(pdf_path):
    with pdfplumber.open(pdf_path) as pdf:
        text = pdf.pages[0].extract_text()
    return text.split("\n")


def parse_decision(pdf_path):
    lines = get_lines(pdf_path)
    result = {}
    current_label = None
    buffer = []

    for line in lines:
        matched_label = None
        for label in LABELS:
            if line.startswith(label) and (len(line) == len(label) or not line[len(label)].islower()):

                matched_label = label
                break

        if matched_label:
            if current_label:
                result[current_label] = " ".join(buffer).strip()
            current_label = matched_label
            buffer =  [line[len(matched_label):].strip()]
        elif current_label:
            buffer.append(line.strip())

    if current_label:
        result[current_label] = " ".join(buffer).strip()

    result["Reason"] = result["Reason"].split("Competitors are reminded")[0].strip()

    return result

if __name__ == "__main__":
    pdf_path = "Reports/test.pdf"
    data = parse_decision(pdf_path)
    for key, value in data.items():
        print(f"{key}: {value}")