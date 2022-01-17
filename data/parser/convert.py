import json


def load_json() -> dict:
    with open("../out.json") as f:
        data = json.load(f)
    return data


def convert(data: dict) -> list:
    r = []
    for datum in data:
        countries_map = datum['countries']
        datum["clist"] = dict([(x['cc'], x['country'].strip()) for x in countries_map])
        datum.pop('countries', None)
        r.append(datum)
    return r


def write_to_file(json_file: list):
    json_object = json.dumps(json_file, indent=4)
    with open("out-cleaned.json", "w") as outfile:
        outfile.write(json_object)


if __name__ == '__main__':
    original_data = load_json()
    converted = convert(original_data)
    write_to_file(converted)
