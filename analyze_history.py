import ujson
import os
from datetime import datetime

files = os.listdir("output/parsed_courses")
for file in files:
    with open("output/parsed_courses/" + file, "r") as f:
        data = ujson.load(f)
    for crn, course in data.items():
        all_events = []
        for type in ["added", "removed", "modified"]:
            for instant in course[type]:
                all_events.append((datetime.fromisoformat(instant["timestamp"]), type))
        all_events.sort()
        is_present = False
        for event in all_events:
            if event[1] == "added":
                if is_present:
                    print(course)
                    raise Exception("Adding twice " + crn + " " + file)
                is_present = True
            elif event[1] == "removed":
                if not is_present:
                    print(course)
                    raise Exception("Removing before adding " + crn + " " + file)
                is_present = False
            elif event[1] == "modified":
                if not is_present:
                    print(course)
                    raise Exception("Modifying before adding " + crn + " " + file)

print("All courses are consistent")
