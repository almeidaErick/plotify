import os
import sqlite3
import json
from functools import wraps

from flask import Flask, Response, g, send_file, send_from_directory, request

import itertools

DATABASE_PATH = os.path.join(os.path.dirname(__file__), 'plotify.db')
PORT = 8080

webapp = Flask(__name__)


def get_db() -> sqlite3.Connection:
    """
    Fetches a request-scoped database connection
    """
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect("file:{}?mode=ro".format(DATABASE_PATH), uri=True)
    return db


@webapp.teardown_appcontext
def close_connection(exception):
    """
    Close database at the end of each request if required
    """
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def json_response(f):
    @wraps(f)
    def inner(*args, **kwargs):
        result = f(*args, **kwargs)
        return Response(json.dumps(result), mimetype="application/json")
    return inner


@webapp.route("/")
def index():
    return send_file("static/index.html")


@webapp.route("/dist/<path:path>")
def static_dist(path):
    return send_from_directory("static/dist", path)

def attributes_factory(cursor, row):
    """
    Custom row factory used for db.row_factory
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d['name'] = row[idx]
    return d

def get_attributes_list(rowFact = False):
    """
    Return all attributes available
    """
    db = get_db()
    if rowFact:
        db.row_factory = attributes_factory
    cursor = db.cursor()
    cursor.execute("select distinct attribute from student_attribute order by attribute")
    return cursor.fetchall()

@webapp.route("/api/attributes")
@json_response
def get_attributes():
    """
    Should fetch a list of unique student attributes

    Response format:
    {
        attributes: [
            {
                name: "...",
            },
            ...
        ]
    }
    """
    attributes = {}
    attributes["attributes"] = get_attributes_list(True)

    return attributes

def filter_data(attribute, data):
    """
    Get the data according to the format

    Unfiltered Data
    [
      ['Teacher name', 'BehaviourEffortMatrix AA', 'BehaviourEffortMatrix AB', .... 'With a disability'],
      ['Albert Einstein', 0, 0, ..., 0],
      ...
      ...
      ...
      ['Tycho Brahe', 0, 0, ..., 0]
    ]

    Filtered Data
    [
      ['Teacher name', 'SINGLE ATTRIBUTE NAME'],
      ['Albert Einstein', 0],
      ...
      ...
      ...
      ['Tycho Brahe', 0]
    ]
    """
    grap_params = []
    chart_data = []
    attribute_list = list(itertools.chain(*get_attributes_list()))
    attribute_dict = {attr: index + 1 for index, attr in enumerate(attribute_list)}

    if attribute is None:
        # Show all data

        # Get teacher name as a reference
        teacher_name = ""

        # Set length of list
        # List structure: ["name of teacher", 0, 0, 0, 0, 0, ....]
        # Each number starting from index 1 represent the ordered attributes, each specific index
        # can be accessed from attribute_dict
        teacher_data = [0]*(len(attribute_list) + 1)
        teacher_data[0] = teacher_name

        # Initialize data list
        grap_params.append("Teacher Name")
        grap_params.extend(attribute_list)
        chart_data.append(grap_params)

        for entry in data:
            # entry[0] -> teacher name
            # entry[1] -> number of students with specific attribute
            # entry[2] -> attribute name
            teacher_name = entry[0]
            if (teacher_data[0] != teacher_name):
                if (teacher_data[0] != ""):
                    # If we get to this point, we've found the name of another teacher
                    chart_data.append(teacher_data)
                teacher_data = [0]*(len(attribute_list) + 1)
                teacher_data[0] = teacher_name
            index_value = attribute_dict[entry[-1]]
            teacher_data[index_value] = entry[1]
        # Add the information for the last teacher in the list
        chart_data.append(teacher_data)

    else:
        # Show filtered data
        grap_params.extend(["Teacher Name", attribute])
        chart_data.append(grap_params)
        for entry in data:
            chart_data.append(list(entry[:2]))

    return chart_data

@webapp.route("/api/chart", methods=["POST"])
@json_response
def get_chart():
    """
    Should fetch the data for the chart
    The request may have POST data

    Response format:
    {
        chartType: ChartType,
        data: [Data],
        options: Options,
    }
    where ChartType, Data, and Options are as demonstrated on https://react-google-charts.com/
    """
    grap_response = {}
    attribute = request.form.get('attribute')
    db = get_db()
    cursor = db.cursor()
    all_data_command = """select cl.teacher_name, COUNT(*), at.attribute as number_of_students
                          from (class as cl inner join (
                          student as st
                          inner join student_attribute as at on st.name = at.student_name) on cl.id = st.class_id)"""

    conditional_section_command = ("where at.attribute = \"{}\"".format(attribute) if attribute != None else '')

    group_by_section_command = """group by cl.teacher_name, at.attribute
                                  order by cl.teacher_name"""

    cursor.execute(all_data_command + conditional_section_command + group_by_section_command)

    grap_response["chartType"] = "ColumnChart"
    grap_response["data"] = filter_data(attribute, cursor.fetchall())
    grap_response["options"] = {
      "title": 'Attribute Distribution for All Teachers',
      "chartArea": { "width": '80%' },
      "hAxis": {
        "minValue": 0,
      },
      "vAxis": {
        "title": 'Number of Students',
      },
      "legend": {"position": "bottom"},
    }

    return grap_response
