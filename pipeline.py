# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import json
import os.path
import seesaw
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import WgetDownload
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
from seesaw.util import find_executable
import shutil
import socket
import sys
import time
import urllib2


# check the seesaw version
if StrictVersion(seesaw.__version__) < StrictVersion("0.1.5"):
    raise Exception("This pipeline needs seesaw version 0.1.5 or higher.")


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_LUA will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string
WGET_LUA = find_executable(
    "Wget+Lua",
    ["GNU Wget 1.14.lua.20130523-9a5c"],
    [
        "./wget-lua",
        "./wget-lua-warrior",
        "./wget-lua-local",
        "../wget-lua",
        "../../wget-lua",
        "/home/warrior/wget-lua",
        "/usr/bin/wget-lua"
    ]
)

if not WGET_LUA:
    raise Exception("No usable Wget+Lua found.")


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = "20140605.00"
USER_AGENT = 'ArchiveTeam'
TRACKER_ID = 'justintv'
TRACKER_HOST = 'tracker.archiveteam.org'
BOUNCER_HOST = 'http://davenport.ninjawedding.org:5985/'


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "CheckIP")

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy
        ip_str = socket.gethostbyname('justin.tv')

        if not ip_str.startswith('192.16.71.'):
            item.log_output('Got IP address: %s' % ip_str)
            item.log_output(
                'Are you behind a firewall/proxy? That is a big no-no!')
            raise Exception(
                'Are you behind a firewall/proxy? That is a big no-no!')


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, "PrepareDirectories")
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item["item_name"]
        escaped_item_name = item_name.encode('hex')
        dirname = "/".join((item["data_dir"], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item["item_dir"] = dirname
        item["warc_file_base"] = "%s-%s-%s" % (self.warc_prefix, escaped_item_name,
            time.strftime("%Y%m%d-%H%M%S"))

        open("%(item_dir)s/%(warc_file_base)s.warc.gz" % item, "w").close()


class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        # NEW for 2014! Check if wget was compiled with zlib support
        if os.path.exists("%(item_dir)s/%(warc_file_base)s.warc"):
            raise Exception('Please compile wget with zlib support!')

        os.rename("%(item_dir)s/%(warc_file_base)s.warc.gz" % item,
              "%(data_dir)s/%(warc_file_base)s.warc.gz" % item)

        shutil.rmtree("%(item_dir)s" % item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()


CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'justintv.lua'))


def stats_id_function(item):
    # NEW for 2014! Some accountability hashes and stats.
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    # Extra specific for justintv project
    status_file = os.path.join(item["item_dir"], "status_info.txt")
    if os.path.exists(status_file):
        with open(status_file) as f:
            d['video_status_code'] = int(f.read())

    return d


class Bouncer(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "Bouncer")

    def process(self, item):
        item_name = item["item_name"]
        response = urllib2.urlopen('{0}/{1}'.format(BOUNCER_HOST, item_name))
        response_data = response.read()
        json_obj = json.loads(response_data)
        item["video_url"] = json_obj['video_url']


class WgetArgs(object):
    def realize(self, item):
        wget_args = [
            WGET_LUA,
            "-U", USER_AGENT,
            "-nv",
            "--lua-script", "justintv.lua",
            "-o", ItemInterpolation("%(item_dir)s/wget.log"),
            "--no-check-certificate",
            "--output-document", ItemInterpolation("%(item_dir)s/wget.tmp"),
            "--truncate-output",
            "-e", "robots=off",
            "--no-cookies",
            "--rotate-dns",
            # "--recursive", "--level=inf",
            "--page-requisites",
            "--timeout", "60",
            "--tries", "inf",
            "--span-hosts",
            "--waitretry", "3600",
            # "--domains", "canv.as,canvasugc.com",
            "--warc-file",
                ItemInterpolation("%(item_dir)s/%(warc_file_base)s"),
            "--warc-header", "operator: Archive Team",
            "--warc-header", "justintv-dld-script-version: " + VERSION,
            "--warc-header", ItemInterpolation("justintv-user: %(item_name)s"),
        ]

        item_name = item['item_name']
        wget_args.append('http://justin.tv/{0}'.format(item_name))
        wget_args.append(item['video_url'])

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)


###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title="Justin.tv",
    project_html="""
        <img class="project-logo" alt="Project logo" src="http://archiveteam.org/images/thumb/9/97/Justintv_logo.png/320px-Justintv_logo.png" height="50px" />
        <h2>Justin.tv <span class="links"><a href="http://justin.tv/">Justin.tv</a> &middot; <a href="http://tracker.archiveteam.org/justintv/">Leaderboard</a></span></h2>
        <p>Justin.tv is deleting all archives sometime in the next week.  We DPoS.</p>
    """,
    utc_deadline=datetime.datetime(2014, 6, 15, 0, 0, 0)
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker("http://%s/%s" % (TRACKER_HOST, TRACKER_ID), downloader,
        VERSION),
    PrepareDirectories(warc_prefix="justintv"),
    Bouncer(),
    WgetDownload(
        WgetArgs(),
        max_tries=5,
        accept_on_exit_code=[0, 8],
        env={
            "item_dir": ItemValue("item_dir")
        }
    ),
    PrepareStatsForTracker(
        defaults={"downloader": downloader, "version": VERSION},
        file_groups={
            "data": [
                ItemInterpolation("%(item_dir)s/%(warc_file_base)s.warc.gz")
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=4, default="1",
        name="shared:rsync_threads", title="Rsync threads",
        description="The maximum number of concurrent uploads."),
        UploadWithTracker(
            "http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation("%(data_dir)s/%(warc_file_base)s.warc.gz")
            ],
            rsync_target_source_path=ItemInterpolation("%(data_dir)s/"),
            rsync_extra_args=[
                "--recursive",
                "--partial",
                "--partial-dir", ".rsync-tmp"
            ]
            ),
    ),
    SendDoneToTracker(
        tracker_url="http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue("stats")
    )
)

