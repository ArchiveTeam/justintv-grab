# encoding=utf8
import datetime
import os
import os.path
import shutil
import json

from seesaw.project import *
from seesaw.config import *
from seesaw.item import *
from seesaw.task import *
from seesaw.pipeline import *
from seesaw.externalprocess import *
from seesaw.tracker import *

project = Project(
  title = "Justin.tv",
  project_html = """
    <img class="project-logo" alt="Project logo" src="http://archiveteam.org/images/thumb/9/97/Justintv_logo.png/320px-Justintv_logo.png" height="50px" />
    <h2>Justin.tv <span class="links"><a href="http://justin.tv/">Justin.tv</a> &middot; <a href="http://tracker.archiveteam.org/justintv/">Leaderboard</a></span></h2>
    <p>Justin.tv is deleting all archives sometime in the next week.  We DPoS.</p>
  """,
  utc_deadline = datetime.datetime(2014,6,8,0,0,0)
)

pipeline = Pipeline(
# SetItemKey("item_name", "1083030"),
  SetItemKey("item_name", StringConfigValue(name="example.item_name", title="Item name", default="1083030")),
  PrintItem(),
  ExternalProcess("Echo", [ "echo", "1234", u"áßðf" ]),
  ExternalProcess("Echo", [ "python", "my_script.py" ]),
  ExternalProcess("sleep", [ "sleep", str(NumberConfigValue(name="example.sleep", title="Time to sleep", description="The example project will sleep n seconds.", min=1, max=15, default="5").value)]),
  ExternalProcess("pwd", [ "pwd" ]),
  PrintItem()
)

