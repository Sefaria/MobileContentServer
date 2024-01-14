import sys
import os
try:
    import re2 as re
except ImportError:
    import re
import json
from tqdm import tqdm
import zipfile
import glob
import time
import traceback
import requests
import errno
from collections import defaultdict
from functools import reduce
from shutil import rmtree
from datetime import timedelta
from datetime import datetime
import dateutil.parser
from concurrent.futures.thread import ThreadPoolExecutor
from local_settings import *

sys.path.insert(0, SEFARIA_PROJECT_PATH)
sys.path.insert(0, SEFARIA_PROJECT_PATH + "/sefaria")
os.environ['DJANGO_SETTINGS_MODULE'] = "sefaria.settings"
import django
django.setup()

import sefaria.model as model
from sefaria.client.wrapper import get_links
from sefaria.model.text import TextChunk, Version
from sefaria.model.schema import Term
from sefaria.utils.calendars import get_all_calendar_items
from sefaria.system.exceptions import InputError, BookNameError
from sefaria.model.history import HistorySet

"""
list all version titles and notes in index
list index of version title / notes in section

OR

put default version title and notes in index
optional var 'merged:true' if merged. In this case section has version title and notes. This can also be merged, in which case ignore version

index is merged if any of its sections are Merged
or
any section has a version different than the default version
"""

SCHEMA_VERSION = "7"  # alternate versions offline
EXPORT_PATH = SEFARIA_EXPORT_PATH + "/" + SCHEMA_VERSION

TOC_PATH          = "/toc.json"
SEARCH_TOC_PATH   = "/search_toc.json"
TOPIC_TOC_PATH    = "/topic_toc.json"
HEB_CATS_PATH     = "/hebrew_categories.json"
PEOPLE_PATH       = "/people.json"
PACK_PATH         = "/packages.json"
CALENDAR_PATH     = "/calendar.json"
LAST_UPDATED_PATH = EXPORT_PATH + "/last_updated.json"
MAX_FILE_SIZE = 100e6
BUNDLE_PATH = f'{EXPORT_PATH}/bundles'

# TODO these descriptions should be moved to the DB
# For now, this data also exists in Sefaria-Project/CalendarsPage.jsx
# Engineers need to be careful to keep these two copies in sync if one of them is edited.
calendarDescriptions = {
    "Parashat Hashavua": {},
    "Haftarah": {
        "en": "The portion from Prophets (a section of the Bible) read on any given week, based on its thematic connection to the weekly Torah portion.",
        "he": "קטע קבוע לכל פרשה מספרי הנביאים הנקרא בכל שבת ומועד, ויש לו קשר רעיוני לפרשת השבוע."
    },
    "Daf Yomi": {
        "en": "A learning program that covers a page of Talmud a day. In this way, the entire Talmud is completed in about seven and a half years.",
        "he": "סדר לימוד לתלמוד הבבלי הכולל לימוד של דף אחד בכל יום. הלומדים בדרך זו מסיימים את קריאת התלמוד כולו בתוך כשבע שנים וחצי.",
        "enSubtitle": "Talmud",
    },
    "929": {
        "en": "A learning program in which participants study five of the Bible’s 929 chapters a week, completing it in about three and a half years.",
        "he": "סדר שבועי ללימוד תנ\"ך שבו נלמדים בכל שבוע חמישה מתוך 929 פרקי התנ\"ך. הלומדים בדרך זו מסיימים את קריאת התנ\"ך כולו כעבור שלוש שנים וחצי.",
        "enSubtitle": "Tanakh",
    },
    "Daily Mishnah": {
        "en": "A program of daily learning in which participants study two Mishnahs (teachings) each day in order to finish the entire Mishnah in six years.",
        "he": "סדר לימוד משנה שבמסגרתו נלמדות שתי משניות בכל יום. הלומדים בדרך זו מסיימים את קריאת המשנה כולה כעבור שש שנים."
    },
    "Daily Rambam": {
        "en": "A learning program that divides Maimonides’ Mishneh Torah legal code into daily units, to complete the whole work in three years.",
        "he": "סדר לימוד הספר ההלכתי של הרמב\"ם, \"משנה תורה\", המחלק את הספר ליחידות יומיות. הלומדים בדרך זו מסיימים את קריאת הספר כולו בתוך שלוש שנים."
    },
    "Daily Rambam (3 Chapters)": {
        "en": "A learning program that divides Maimonides’ Mishneh Torah legal code into daily units, to complete the whole work in one year.",
        "he": "סדר לימוד הספר ההלכתי של הרמב\"ם, \"משנה תורה\", המחלק את הספר ליחידות יומיות. הלומדים בדרך זו מסיימים את קריאת הספר כולו בתוך שנה אחת.",
    },
    "Daf a Week": {
        "en": "A learning program that covers a page of Talmud a week. By going at a slower pace, it facilitates greater mastery and retention.",
        "he": "סדר שבועי ללימוד התלמוד הבבלי שבו נלמד דף תלמוד אחד בכל שבוע. קצב הלימוד האיטי מאפשר ללומדים הפנמה ושליטה רבה יותר בחומר הנלמד.",
        "enSubtitle": "Talmud",
    },
    "Halakhah Yomit": {
        "en": "A four year daily learning program in which participants study central legal texts that cover most of the daily and yearly rituals.",
        "he": "תוכנית ארבע־שנתית ללימוד מקורות הלכתיים מרכזיים העוסקים ברוב הלכות היום־יום והמועדים.",
    },
    "Arukh HaShulchan Yomi": {
        "en": "A four-year daily learning program covering ritual halakhot, practical kashrut and interpersonal mitzvot within Rabbi Yechiel Michel Epstein’s legal code, Arukh HaShulchan.",
        "he": "תכנית לימוד ארבע-שנתית של הלכות מעשיות מתוך ספר ערוך השלחן, חיבורו ההלכתי של הרב יחיאל מיכל עפשטיין.",
    },
    "Tanya Yomi": {
        "en": "A daily learning cycle for completing Tanya annually, starting at the 19th of Kislev, “Rosh Hashanah of Chasidut.”",
        "he": "סדר לימוד המשלים את ספר התניא אחת לשנה, החל מיום י\"ט בכסליו \"ראש השנה לחסידות\"."
    },
    "Tanakh Yomi": {
        "en": "A daily learning cycle for completing Tanakh annually. On Shabbat, each Torah portion is recited. On weekdays, Prophets and Writings are recited according to the ancient Masoretic division of sedarim.",
        "he": "סדר לימוד המשלים את קריאת התנ\"ך כולו אחת לשנה. בשבתות קוראים את התורה לפי סדר פרשיות השבוע. בימות החול קוראים את הנ\"ך על פי חלוקת הסדרים של המסורה.",
        "enSubtitle": "Tanakh",
    },
    "Zohar for Elul": {
        "en": "A 40 day learning schedule in which participants learn the Kabbalistic work \"Tikkunei Zohar\" over the course of the days between the First of the month of Elul and Yom Kippur.",
        "he": "סדר יומי ללימוד הספר \"תיקוני הזהר\". הסדר נמשך 40 יום, בתקופה שבין ראש חודש אלול ויום הכיפורים.",
    },
    "Chok LeYisrael": {
        "en": "A book designed for daily study. Each day’s learning consists of biblical verses together with commentary, a chapter of Mishnah, and short passages of Talmud, Zohar, and of works of Halakhah and Musar.",
        "he": 'לימוד יומי הכולל פסוקי תנ״ך ופירושם, פרק משנה, קטע מהתלמוד, קטע מהזוהר, קטע מספר הלכה וקטע מספר מוסר.',
    }
}


def keep_directory(func):
    def new_func(*args, **kwargs):
        original_dir = os.getcwd()
        try:
            return func(*args, **kwargs)
        finally:
            os.chdir(original_dir)
    return new_func


def write_doc(doc, path):
    """
    Takes a dictionary `doc` ready to export and actually writes the file to the filesystem.
    """

    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    with open(path, "w") as f:
        json.dump(doc, f, ensure_ascii=False, indent=(None if MINIFY_JSON else 4), separators=((',',':') if MINIFY_JSON else None))


@keep_directory
def os_error_cleanup():
    os.chdir(EXPORT_PATH)
    json_files = glob.glob('*.json')

    for jf in json_files:
        if not re.search(r'(calendar|toc|last_updated|hebrew_categories).json', jf):
            os.remove(jf)
    message = 'OSError during export'
    print(message)
    alert_slack(message, ':redlight:')
    clear_old_bundles(max_files=0)

def alert_slack(message, icon_emoji):
    if DEBUG_MODE:
        print(message)
        return
    try:
        slack_url = os.environ['SLACK_URL']
    except KeyError:
        return
    requests.post(slack_url, json={
        'channel': '#engineering-mobile',
        'text': message,
        'username': 'Mobile Export',
        'icon_emoji': icon_emoji
    })

@keep_directory
def zip_last_text(title):
    """
    Zip up the JSON files of the last text exported into and delete the original JSON files.
    Assumes that all previous JSON files have been deleted and the remaining ones should go in the new zip.
    """
    os.chdir(EXPORT_PATH)

    zip_path = f"{EXPORT_PATH}/{title}.zip"

    z = zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED)

    for file in glob.glob(f"./{title}*.json"):
        z.write(file)
        os.remove(file)
    z.close()


def export_texts(skip_existing=False):
    """
    Exports all texts in the database.
    TODO -- check history and last_updated to only export texts with changes
    """
    indexes = model.library.all_index_records()
    for index in tqdm(reversed(indexes), desc='export all', total=len(indexes), file=sys.stdout):
        if skip_existing and os.path.isfile("%s/%s.zip" % (EXPORT_PATH, index.title)):
            continue

        start_time = time.time()
        success = export_text(index)
        if not success:
            indexes.remove(index)
        tqdm.write(f"--- {index.title} - {round(time.time() - start_time, 2)} seconds ---")

    write_last_updated([i.title for i in indexes])


@keep_directory
def export_text(index, update=False):
    """Writes a ZIP file containing text content json and text index JSON
    :param index: can be either Index or str
    :param update: True if you want to write_last_updated for just this index
    """
    if isinstance(index, str):
        index = model.library.get_index(index)

    success = export_text_json(index)
    success = export_index(index) and success
    zip_last_text(index.title)

    if update and success:
        write_last_updated([index.title], update=update)

    return success


def export_updated():
    """
    Writes new TOC and zip files for each text that has changed since the last update.
    Write update times to last updated list.
    """
    #edit text, add text, edit text: {"date" : {"$gte": ISODate("2017-01-05T00:42:00")}, "ref" : /^Rashi on Leviticus/} REMOVE NONE INDEXES
    #add link, edit link: {"rev_type": "add link", "new.refs": /^Rashi on Berakhot/} REMOVE NONE INDEXES
    #delete link, edit link: {"rev_type": "add link", "old.refs": /^Rashi on Berakhot/} REMOVE NONE INDEXES
    if not os.path.exists(LAST_UPDATED_PATH):
        export_all()
        return

    print("Generating updated books list.")
    updated_books = updated_books_list()
    print("{} books updated.".format(len(updated_books)))
    new_books = new_books_since_last_update()
    print("{} books added.".format(len(new_books)))
    updated_books += new_books

    print("Updating {} books\n{}".format(len(updated_books), "\n\t".join(updated_books)))
    updated_indexes = []
    for t in updated_books:
        try:
            updated_indexes += [model.library.get_index(t)]
        except BookNameError:
            print("Skipping update for non-existent book '{}'".format(t))

    updated_books = [x.title for x in updated_indexes]
    for index in tqdm(updated_indexes, desc='export updated'):
        success = export_text(index)
        if not success:
            updated_books.remove(index.title) # don't include books which dont export

    export_toc()
    export_topic_toc()
    export_hebrew_categories()
    export_packages()
    export_calendar()
    export_authors()
    write_last_updated(updated_books, update=True)


def updated_books_list():
    """
    Returns a list of books that have updated since the last export.
    Returns None is there is no previous last_updated.json
    """
    if not os.path.exists(LAST_UPDATED_PATH):
        return None
    last_updated = json.load(open(LAST_UPDATED_PATH, "rb")).get("titles", {})
    updated_books = [x[0] for x in [x for x in list(last_updated.items()) if has_updated(x[0], dateutil.parser.parse(x[1]))]]
    return updated_books


def has_updated(title, last_updated):
    """
    title - str name of index
    last_updated - datetime obj of our current knowledge of when this title was last updated
    """
    def construct_query(attribute, queries):
        query_list = [{attribute: {'$regex': query}} for query in queries]
        return {"date": {"$gt": last_updated}, '$or': query_list}
    try:
        title_queries = model.Ref(title).regex(as_list=True)
    except InputError:
        return False

    text_count = HistorySet(construct_query("ref", title_queries)).count()
    if text_count > 0:
        return True

    old_link_count = HistorySet(construct_query("old.refs", title_queries)).count()
    if old_link_count > 0:
        return True

    new_link_count = HistorySet(construct_query("new.refs", title_queries)).count()
    if new_link_count > 0:
        return True

    index_count = HistorySet({"date": {"$gt": last_updated}, "title":title}).count()
    if index_count > 0:
        return True

    return False


def should_include_all_versions(index):
    return index.get_primary_corpus() == "Tanakh"


def export_text_json(index):
    """
    Takes a single document from the `texts` collection exports it, by chopping it up
    Add helpful data like

    returns True if export was successful
    """
    try:
        index_exporter = IndexExporter(index, include_all_versions=should_include_all_versions(index))
        for oref in index.all_top_section_refs():
            if oref.is_section_level():
                # depth 2 (or 1?)
                text_by_version, metadata = index_exporter.section_data(oref)
            else:
                sections = oref.all_subrefs()
                metadata = {
                    "ref": oref.normal(),
                    "sections": {},
                }
                text_by_version = defaultdict(lambda: {
                    "ref": oref.normal(),
                    "sections": {},
                })
                for section in sections:
                    if section.is_section_level():
                        # depth 3
                        # doc["sections"][section.normal()]
                        curr_text_by_version, curr_metadata = index_exporter.section_data(section)
                        metadata["sections"][section.normal()] = curr_metadata
                        for vtitle, text_array in curr_text_by_version.items():
                            text_by_version[vtitle]["sections"][section.normal()] = text_array
                    else:
                        # depth 4
                        real_sections = section.all_subrefs()
                        for real_section in real_sections:
                            curr_text_by_version, curr_metadata = index_exporter.section_data(real_section)
                            metadata["sections"][real_section.normal()] = curr_metadata
                            for vtitle, text_array in curr_text_by_version.items():
                                text_by_version[vtitle]["sections"][real_section.normal()] = text_array

            for (vtitle, lang), data in text_by_version.items():
                path = make_path(vtitle, lang, metadata['ref'])
                write_doc(data, path)
            write_doc(metadata, f"{EXPORT_PATH}/{metadata['ref']}.metadata.json")
        return True

    except OSError as e:
        if e.errno == errno.ENOSPC:
            # disk out of space, try to clean up a little
            os_error_cleanup()
            raise OSError
        print("Error exporting %s: %s" % (index.title, e))
        print(traceback.format_exc())
        return False
    except Exception as e:
        print("Error exporting %s: %s" % (index.title, e))
        print(traceback.format_exc())
        return False


def get_version_hash(version_title):
    import hashlib
    return hashlib.md5(version_title.encode()).hexdigest()[:8]


def make_path(version_title, lang, tref):
    return f"{EXPORT_PATH}/{tref}.{get_version_hash(version_title)}.{lang}.json"


def simple_link(link):
    """
    Returns dictionary with all we care about for link in a section
    """
    simple = {
        "sourceHeRef": link["sourceHeRef"],
        "sourceRef":   link["sourceRef"]
    }
    if link["category"] in ("Quoting Commentary", "Targum"):
        simple["category"] = link["category"]
    if link.get("sourceHasEn", False):  # only include when True
        simple["sourceHasEn"] = True
    return simple


class IndexExporter:

    def __init__(self, index_obj: model.Index, include_all_versions=False):
        self._text_map = {}
        self.version_state = index_obj.versionState()
        leaf_nodes = index_obj.nodes.get_leaf_nodes()
        for leaf in leaf_nodes:
            oref = leaf.ref()
            chunks = []
            all_versions = oref.versionset()
            if include_all_versions:
                for version in all_versions:
                    chunks += [oref.text(version.language, version.versionTitle)]
            else:
                chunks = [oref.text('en'), oref.text('he')]
            chunks = [c for c in chunks if not c.is_empty()]

            self._text_map[leaf.full_title()] = {
                'chunks': chunks,
                'all_versions': all_versions,
                'jas': [c.ja() for c in chunks],
            }

    @staticmethod
    def get_text_array(sections, ja):
        if sections:
            try:
                return ja.get_element([j-1 for j in sections])
            except IndexError:
                return []
        else:
            # Ref(Pesach Haggadah, Kadesh) does not have sections, although it is a section ref
            return ja.array()

    @staticmethod
    def strip_itags_recursive(text_array):
        if isinstance(text_array, str):
            return TextChunk.strip_itags(text_array)
        else:
            return [IndexExporter.strip_itags_recursive(sub_text_array) for sub_text_array in text_array]

    @staticmethod
    def serialize_version_details(version):
        return {'versionTitle': version.versionTitle, 'language': version.language}

    def serialize_all_version_details(self, versions):
        return [self.serialize_version_details(version) for version in versions]

    @staticmethod
    def pad_array_to_index(array, index):
        while len(array) != index - 1:
            array.append(None)
        return array

    @staticmethod
    def _get_anchor_ref_dict(oref, section_length):
        section_links = get_links(oref.normal(), False)
        anchor_ref_dict = defaultdict(list)
        for link in section_links:
            anchor_oref = model.Ref(link["anchorRef"])
            if not anchor_oref.is_segment_level() or len(anchor_oref.sections) == 0:
                continue  # don't bother with section level links
            start_seg_num = anchor_oref.sections[-1]
            # make sure sections are the same in range
            # TODO doesn't deal with links that span sections
            end_seg_num = anchor_oref.toSections[-1] if anchor_oref.sections[0] == anchor_oref.toSections[0] else section_length
            for x in range(start_seg_num, end_seg_num+1):
                anchor_ref_dict[x] += [simple_link(link)]
        return anchor_ref_dict

    @staticmethod
    def _get_base_file_name(tref, version_title):
        return f"{tref}.{get_version_hash(version_title)}.json"

    def section_data(self, oref: model.Ref):
        """
        :param oref: section level Ref instance
        :param prev_next: tuple, with the oref before oref and after oref (or None if this is the first/last ref)
        Returns a dictionary with all the data we care about for section level `oref`.
        """
        prev, next_ref = oref.prev_section_ref(vstate=self.version_state),\
                         oref.next_section_ref(vstate=self.version_state)

        node_title = oref.index_node.full_title()
        metadata = {
            "ref": oref.normal(),
            "heRef": oref.he_normal(),
            "indexTitle": oref.index.title,
            "heTitle": oref.index.get_title('he'),
            "sectionRef": oref.normal(),
            "next": next_ref.normal() if next_ref else None,
            "prev": prev.normal() if prev else None,
            "versions": self.serialize_all_version_details(self._text_map[node_title]['all_versions'])
        }

        jas = self._text_map[node_title]['jas']
        text_arrays = [
            self.strip_itags_recursive(self.get_text_array(oref.sections, ja)) for ja in jas
        ]

        section_length = max(len(a) for a in text_arrays)
        anchor_ref_dict = self._get_anchor_ref_dict(oref, section_length)
        offset = oref._get_offset([sec-1 for sec in oref.sections])
        text_serialized_list = [[] for _ in text_arrays]
        links_serialized = []
        for x in range(0, section_length):
            curr_seg_num = x + offset + 1
            links = anchor_ref_dict[x+1]
            if len(links) > 0:
                links_serialized = self.pad_array_to_index(links_serialized, curr_seg_num)
                links_serialized += [links]

            for iarray, text_array in enumerate(text_arrays):
                if x >= len(text_array):
                    continue
                serialized_array = text_serialized_list[iarray]
                self.pad_array_to_index(serialized_array, curr_seg_num)
                serialized_array.append(text_array[x])
        metadata['links'] = links_serialized

        text_by_version = {}
        chunks = self._text_map[node_title]['chunks']
        for i, serialized_text in enumerate(text_serialized_list):
            vdeets = self.get_version_details(chunks[i])
            text_by_version[vdeets] = serialized_text
        return text_by_version, metadata

    @staticmethod
    def get_version_details(chunk):
        if not chunk.is_merged:
            version = chunk.version()
            return version.versionTitle, version.language
        # merged
        versions_by_title = {v.versionTitle: v for v in chunk._versions}
        top_version_title = max(chunk.sources, key=lambda vtitle: getattr(versions_by_title[vtitle], 'priority', -1))
        return top_version_title, chunk.lang


def export_index(index):
    """
    Writes the JSON of the index record of the text called `title`.
    """
    try:
        serialized_index = index.contents_with_content_counts()
        annotate_versions_on_index(index.title, serialized_index)
        path = f"{EXPORT_PATH}/{index.title}_index.json"
        write_doc(serialized_index, path)

        return True
    except OSError:
        os_error_cleanup()
        raise OSError
    except Exception as e:
        print("Error exporting index for %s: %s" % (index.title, e))
        print(traceback.format_exc())

        return False


def annotate_versions_on_index(title, serialized_index: dict):
    vkeys_to_remove = ['versionTitle', 'versionNotes', 'license', 'versionSource', 'versionTitleInHebrew', 'versionNotesInHebrew']
    for key in vkeys_to_remove:
        serialized_index.pop(key, None)
        he_key = 'he' + key[0].capitalize() + key[1:]
        serialized_index.pop(he_key, None)
    serialized_index['versions'] = model.Ref(title).version_list()

    # remove empty values
    for version in serialized_index['versions']:
        version_items = list(version.items())
        for key, value in version_items:
            if isinstance(value, str) and len(value) == 0:
                version.pop(key, None)


def get_indexes_in_category(cats, toc):
    indexes = []
    category_found = False
    for temp_toc in toc:
        if "contents" in temp_toc and (len(cats) == 0 or temp_toc["category"] == cats[0]):
            category_found = True
            if len(temp_toc["contents"]) == 0: continue
            indexes += get_indexes_in_category(cats[1:], temp_toc["contents"])
        elif len(cats) == 0 and "title" in temp_toc:
            indexes += [temp_toc["title"]]
            category_found = True
    if not category_found:
        raise InputError
    return indexes


def get_downloadable_packages():
    toc = clean_toc_nodes(model.library.get_toc())
    packages = [
        {
            "en": "COMPLETE LIBRARY",
            "he": "כל הספרייה",
            "color": "Other",
            "categories": []
        },
        {
            "en": "TANAKH with Rashi",
            "he": "תנ״ך עם רש״י",
            "color": "Tanakh",
            "parent": "TANAKH and all commentaries",
            "categories": [
                "Tanakh/Torah",
                "Tanakh/Prophets",
                "Tanakh/Writings",
                "Tanakh/Rishonim on Tanakh/Rashi"
            ]
        },
        {
            "en": "TANAKH and all commentaries",
            "he": "תנ״ך וכל המפרשים",
            "color": "Tanakh",
            "categories": [
                "Tanakh"
            ]
        },
        {
            "en": "TALMUD with Rashi and Tosafot",
            "he": "תלמוד עם רש״י ותוספות",
            "parent": "TALMUD and all commentaries",
            "color": "Talmud",
            "categories": [
                "Talmud/Bavli/Seder Zeraim",
                "Talmud/Bavli/Seder Moed",
                "Talmud/Bavli/Seder Nashim",
                "Talmud/Bavli/Seder Nezikin",
                "Talmud/Bavli/Seder Kodashim",
                "Talmud/Bavli/Seder Tahorot",
                "Talmud/Bavli/Rishonim on Talmud/Rashi",
                "Talmud/Bavli/Rishonim on Talmud/Tosafot"
            ]
        },
        {
            "en": "TALMUD and all commentaries",
            "he": "תלמוד וכל המפרשים",
            "color": "Talmud",
            "categories": [
                "Talmud"
            ]
        }
    ]
    # Add all top-level categories
    for cat in toc[:7]:
        if cat["category"] == "Tanakh" or cat["category"] == "Talmud":
            continue  # already included above
        packages += [{
            "en": cat["category"].upper(),
            "he": cat["heCategory"],
            "color": cat["category"],
            "categories": [cat["category"]]
        }]
    for p in packages:
        indexes = []
        hasCats = len(p["categories"]) > 0
        if hasCats:
            for c in p["categories"]:
                try:
                    indexes += get_indexes_in_category(c.split("/"), toc)
                except InputError:
                    alert_slack(f"Error in `get_downloadable_packages()`. Category doesn't exist: {c}", ':redlight:')
        else:
            try:
                indexes += get_indexes_in_category([], toc)
            except InputError:
                alert_slack(f"Error in `get_downloadable_packages()`. Full library", ':redlight:')
        size = 0
        for i in indexes:
            size += os.path.getsize("{}/{}.zip".format(EXPORT_PATH, i)) if os.path.isfile("{}/{}.zip".format(EXPORT_PATH, i)) else 0  # get size in kb. overestimate by 1kb
        if hasCats:
            # only include indexes if not complete library
            p["indexes"] = indexes
        del p["categories"]
        p["size"] = size
    return packages


def write_last_updated(titles, update=False):
    """
    Writes to `last_updated.json` the current time stamp for all `titles`.
    :param update: True if you only want to update the file and not overwrite
    """
    def get_timestamp(title):
        return datetime.fromtimestamp(os.stat(f'{EXPORT_PATH}/{title}.zip').st_mtime).isoformat()

    if not titles:
        titles = filter(lambda x: x.endswith('zip'), os.listdir(EXPORT_PATH))
        titles = [re.search(r'([^/]+)\.zip$', title).group(1) for title in titles]

    last_updated = {
        "schema_version": SCHEMA_VERSION,
        "comment": "",
        "titles": {
            title: get_timestamp(title)
            for title in titles
        }
    }
    #last_updated["SCHEMA_VERSION"] = SCHEMA_VERSION
    if update:
        try:
            old_doc = json.load(open(LAST_UPDATED_PATH, "rb"))
        except IOError:
            old_doc = {"schema_version": 0, "comment": "", "titles": {}}

        old_doc["schema_version"] = last_updated["schema_version"]
        old_doc["comment"] = last_updated["comment"]
        old_doc["titles"].update(last_updated["titles"])
        last_updated = old_doc

    write_doc(last_updated, LAST_UPDATED_PATH)

    if USE_CLOUDFLARE:
        purge_cloudflare_cache(titles)


def export_packages(for_sources=False):
    packages = get_downloadable_packages()
    write_doc(packages, (SEFARIA_IOS_SOURCES_PATH if for_sources else EXPORT_PATH) + PACK_PATH)
    write_doc(packages, (SEFARIA_ANDROID_SOURCES_PATH if for_sources else EXPORT_PATH) + PACK_PATH)


def split_list(l, size):
    chunks = int(len(l) / size)
    values = [l[i*size:(i+1)*size] for i in range(chunks)]
    last_bit = l[chunks*size:len(l)]
    if last_bit:
        values.append(last_bit)
    return values


def build_split_archive(book_list, build_loc, export_dir='', archive_size=MAX_FILE_SIZE):
    if os.path.exists(build_loc):
        try:
            rmtree(build_loc)
        except NotADirectoryError:
            os.remove(build_loc)
    os.mkdir(build_loc)
    z, current_size, i, filenames = None, 0, 0, []
    for title in book_list:
        if not z:
            i += 1
            filename = f'{build_loc}/{i}.zip'
            z = zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED)
            current_size = 0
            filenames.append(filename)
        try:
            z.write(os.path.join(export_dir, title), arcname=title)
        except FileNotFoundError:
            print(f"No zip file for {title}; the bundles will be missing this text")
            continue
        current_size += z.getinfo(title).compress_size
        if current_size > archive_size:
            z.close()
            z = None
    if z:
        z.close()

    return [os.path.basename(f) for f in filenames]


@keep_directory
def clear_old_bundles(max_files=50):
    """
    This method will check the bundles directory and clean out old bundles (this would be updates that aren't being used)
    :param old_age: bundles not served in this number of days will be deleted
    :param max_files: if less than this many files exist, nothing will happen
    :return:
    """
    os.chdir(BUNDLE_PATH)
    # get packages
    with open('../packages.json') as fp:
        packages = json.load(fp)
    packages = set(p['en'] for p in packages)
    # list all non package bundles
    bundles = [s for s in os.listdir('.') if s not in packages]
    if len(bundles) < max_files:
        return
    # for each package if old, delete
    for bundle in bundles:
        try:
            rmtree(bundle)
        except FileNotFoundError:
            pass


@keep_directory
def zip_packages():
    packages = get_downloadable_packages()
    bundle_path = BUNDLE_PATH
    if not os.path.isdir(bundle_path):
        os.mkdir(bundle_path)

    curdir = os.getcwd()
    os.chdir(EXPORT_PATH)

    for package in packages:
        package_name = package['en']
        print(package_name)
        if package_name == 'COMPLETE LIBRARY':
            titles = [i.title for i in model.library.all_index_records()]
        else:
            titles = package['indexes']
        titles = [f'{t}.zip' for t in titles]
        build_split_archive(titles, f'{bundle_path}/{package_name}')

    os.chdir(curdir)


def export_hebrew_categories(for_sources=False):
    """
    Writes translation of all English categories into a single file.
    """
    print("Export Hebrew Categories")
    term = Term()
    eng_cats = model.library.get_text_categories()
    hebrew_cats_json = {}
    for e in eng_cats:
        t = term.load_by_title(e)
        if not t:
            print("Couldn't load term '{}'. Skipping Hebrew category".format(e))
        else:
            hebrew_cats_json[e] = t.titles[1]['text']
    write_doc(hebrew_cats_json, (SEFARIA_IOS_SOURCES_PATH if for_sources else EXPORT_PATH) + HEB_CATS_PATH)
    write_doc(hebrew_cats_json, (SEFARIA_ANDROID_SOURCES_PATH if for_sources else EXPORT_PATH) + HEB_CATS_PATH)


def export_topic_toc(for_sources=False):
    topic_toc = model.library.get_topic_toc_json_recursive(with_descriptions=True)
    write_doc(topic_toc, (SEFARIA_IOS_SOURCES_PATH if for_sources else EXPORT_PATH) + TOPIC_TOC_PATH)
    write_doc(topic_toc, (SEFARIA_ANDROID_SOURCES_PATH if for_sources else EXPORT_PATH) + TOPIC_TOC_PATH)

def clean_toc_nodes(toc):
    """
    Removes any nodes in TOC that we can't handle.
    """
    newToc = []
    for t in toc:
        if "contents" in t:
            new_item = {}
            for k, v in list(t.items()):
                if k != "contents":
                    new_item[k] = v
            newToc += [new_item]
            newToc[-1]["contents"] = clean_toc_nodes(t["contents"])
        elif "isGroup" in t or t.get('isCollection', False):
            continue  # Not currently handling sheets in TOC
        elif "title" in t:
            newToc += [{k: v for k, v in list(t.items())}]
        else:
            print("Goodbye {}".format(t))
    return newToc



def export_toc(for_sources=False):
    """
    Writes the Table of Contents JSON to a single file.
    """
    print("Export Table of Contents")
    new_toc = model.library.get_toc_tree(mobile=True, rebuild=True).get_serialized_toc()
    new_search_toc = model.library.get_search_filter_toc()
    new_new_toc = clean_toc_nodes(new_toc)
    new_new_search_toc = clean_toc_nodes(new_search_toc)
    write_doc(new_new_toc, (SEFARIA_IOS_SOURCES_PATH if for_sources else EXPORT_PATH) + TOC_PATH)
    write_doc(new_new_search_toc, (SEFARIA_IOS_SOURCES_PATH if for_sources else EXPORT_PATH) + SEARCH_TOC_PATH)
    write_doc(new_new_toc, (SEFARIA_ANDROID_SOURCES_PATH if for_sources else EXPORT_PATH) + TOC_PATH)
    write_doc(new_new_search_toc, (SEFARIA_ANDROID_SOURCES_PATH if for_sources else EXPORT_PATH) + SEARCH_TOC_PATH)


def new_books_since_last_update():
    """
    Returns a list of books that have been added to the library since the last update.
    """
    new_toc = clean_toc_nodes(model.library.get_toc())
    def get_books(temp_toc, books):
        if isinstance(temp_toc, list):
            for child_toc in temp_toc:
                if "contents" in child_toc:
                    child_toc = child_toc["contents"]
                books.update(get_books(child_toc, set()))
        else:
            try:
                books.add(temp_toc["title"])
            except KeyError:
                print("Bad Toc item skipping {}".format(temp_toc))
        return books

    last_updated = json.load(open(LAST_UPDATED_PATH, 'rb')) if os.path.exists(LAST_UPDATED_PATH) else {"titles": {}}
    old_books = list(last_updated["titles"].keys())
    new_books = get_books(new_toc, set())

    added_books = [book for book in new_books if book not in old_books]
    return added_books


def unnormalize_talmud_ranges(tref):
    oref = model.Ref(tref)
    iaddr = len(oref.sections)-1
    if oref.index_node.addressTypes[iaddr] == 'Talmud' and oref.range_size() > 1:
        section_refs = oref.split_spanning_ref()
        return f"{section_refs[0].normal()}-{model.schema.AddressTalmud.toStr('en', section_refs[-1].sections[iaddr])}"
    return tref


def _get_calendar_metadata():
    metadata = {}
    for calendar_name, temp_metadata in calendarDescriptions.items():
        description = reduce(lambda a, lang: {**a, lang: temp_metadata[lang]} if lang in temp_metadata else a, ("en", "he"), {})
        subtitle = reduce(lambda a, lang: {**a, lang: temp_metadata.get(f"{lang}Subtitle", None)} if f"{lang}Subtitle" in temp_metadata else a, ("en", "he"), {})
        metadata[calendar_name] = {
            "description": description,
            "subtitle": subtitle,
        }
    return metadata


def _get_custom_shorthand(title):
    pattern = '\s\(([A-Z\u05d0-\u05ea]+)\)$'
    match = re.search(pattern, title)
    if match:
        title_without_custom = re.sub(pattern, "", title)
        custom = match.group(1)
        return title_without_custom, custom
    return None, None


def _pull_out_custom_shorthand(c):
    if c['title']['en'].startswith("Haftarah "):
        c['custom_shorthand'] = {}
        for lang in ('en', 'he'):
            title_without_custom, custom_shorthand = _get_custom_shorthand(c['title'][lang])
            c['title'][lang] = title_without_custom
            c['custom_shorthand'][lang] = custom_shorthand
    return c


def export_calendar(for_sources=False):
    """
    Writes a JSON file with all calendars from `get_all_calendar_items` for the next 365 days
    """
    calendar = {'metadata': _get_calendar_metadata()}
    base = datetime.today()
    date_list = [base + timedelta(days=x) for x in range(-2, 365)]
    for dt in date_list:
        curr_cal = defaultdict(list)
        all_possibilities = defaultdict(lambda: defaultdict(list))
        for diaspora in (True, False):
            for custom in ('ashkenazi', 'sephardi'):
                cal_items = get_all_calendar_items(dt, diaspora=diaspora, custom=custom)
                # aggregate by type to combine refs
                cal_items_dict = {}
                for c in cal_items:
                    has_ref = bool(c.get('ref', False))  # some calendar items only have URLs (e.g. Chok Leyisrael)
                    c['hasRef'] = has_ref
                    ckey = c['order']
                    if has_ref:
                        c['ref'] = unnormalize_talmud_ranges(c['ref'])
                    if ckey in cal_items_dict:
                        # not currently supporting multiple URLs for one calendar item
                        if has_ref:
                            cal_items_dict[ckey]['refs'] += [c['ref']]
                        cal_items_dict[ckey]['subs'] += [c['displayValue']]
                    else:
                        if has_ref:
                            c['refs'] = [c['ref']]
                            del c['url']
                            del c['ref']
                        displayValue = c['displayValue']
                        del c['displayValue']
                        c['subs'] = [displayValue]
                        cal_items_dict[ckey] = c
                for ckey, c in list(cal_items_dict.items()):
                    c['custom'] = custom
                    c['diaspora'] = diaspora
                    c = _pull_out_custom_shorthand(c)
                    cid = c.get('refs', [])[0] if c['hasRef'] else c.get('url', '')
                    del c['hasRef']
                    all_possibilities[ckey][cid] += [c]
        for key, title_dict in list(all_possibilities.items()):
            for i, (tref, poss_list) in enumerate(title_dict.items()):
                if i == 0:
                    del poss_list[0]['custom']
                    del poss_list[0]['diaspora']
                    curr_cal['d'] += [poss_list[0]]
                else:
                    for p in poss_list:
                        pkey = '{}|{}'.format(1 if p['diaspora'] else 0, p['custom'][0])
                        del p['custom']
                        del p['diaspora']
                        curr_cal[pkey] += [p]
        calendar[dt.date().isoformat()] = curr_cal

    path = (SEFARIA_IOS_SOURCES_PATH if for_sources else EXPORT_PATH) + CALENDAR_PATH
    write_doc(calendar, path)
    write_doc(calendar, (SEFARIA_ANDROID_SOURCES_PATH if for_sources else EXPORT_PATH) + CALENDAR_PATH)


def export_authors(for_sources=False):
    ps = model.AuthorTopicSet()
    people = {}
    for person in ps:
        for title in person.titles:
            people[title["text"].lower()] = 1
    path = (SEFARIA_IOS_SOURCES_PATH if for_sources else EXPORT_PATH) + PEOPLE_PATH
    write_doc(people, path)
    write_doc(people, (SEFARIA_ANDROID_SOURCES_PATH if for_sources else EXPORT_PATH) + PEOPLE_PATH)


def clear_exports():
    """
    Deletes all files from any export directory listed in export_formats.
    """
    if os.path.exists(EXPORT_PATH):
        rmtree(EXPORT_PATH)


def recursive_listdir(path):
    file_list, sub_dirs = [], []

    def recurse(r_path):
        nonlocal file_list, sub_dirs
        for f in os.listdir(r_path):
            if os.path.isfile(f'{r_path}/{f}'):
                file_list.append(f'{"/".join(sub_dirs)}/{f}')
            else:
                sub_dirs.append(f)
                recurse(f'{r_path}/{f}')
                sub_dirs.pop()
    recurse(path)
    return file_list


def iter_chunks(list_obj: list, chunk_size: int):
    current_loc = 0
    while current_loc < len(list_obj):
        yield list_obj[current_loc:current_loc + chunk_size]
        current_loc = current_loc + chunk_size


def purge_cloudflare_cache(titles):
    """
    Purges the URL for each zip file named in `titles` as well as toc.json, last_updated.json and calendar.json.
    """
    if not titles:
        titles = [t.title for t in model.library.all_index_records()]
    files = ["%s/%s/%s.zip" % (CLOUDFLARE_PATH, SCHEMA_VERSION, title) for title in titles]
    files += ["%s/%s/%s.json" % (CLOUDFLARE_PATH, SCHEMA_VERSION, title) for title in ("toc", "topic_toc", "search_toc", "last_updated", "calendar", "hebrew_categories", "people", "packages")]
    files += [f'{CLOUDFLARE_PATH}/{SCHEMA_VERSION}/{f}' for f in recursive_listdir(f'./static/ios-export/{SCHEMA_VERSION}/bundles')]
    url = 'https://api.cloudflare.com/client/v4/zones/%s/purge_cache' % CLOUDFLARE_ZONE

    def send_purge(file_list):
        payload = {"files": file_list}
        headers = {
            "X-Auth-Email": CLOUDFLARE_EMAIL,
            "X-Auth-Key": CLOUDFLARE_TOKEN,
            "Content-Type": "application/json",
        }
        return requests.delete(url, data=json.dumps(payload), headers=headers)

    with ThreadPoolExecutor() as executor:
        results = executor.map(send_purge, iter_chunks(files, 25))
    # r = requests.delete(url, data=json.dumps(payload), headers=headers)
    print("Purged {} files from Cloudflare".format(len(files)))

    return results


def export_all(skip_existing=False):
    """
    Export everything we need.
    If `skip_existing`, skip any text that already has a zip file, otherwise delete everything and start fresh.
    """
    start_time = time.time()
    export_toc()
    export_topic_toc()
    export_calendar()
    export_hebrew_categories()
    export_texts(skip_existing)
    export_authors()
    export_packages()
    print(("--- %s seconds ---" % round(time.time() - start_time, 2)))


def export_base_files_to_sources():
    """
    Export the basic files that should be bundled with a new release of the iOS app
    Run this before every new release
    """
    export_toc(for_sources=True)
    export_topic_toc(for_sources=True)
    export_hebrew_categories(for_sources=True)
    export_calendar(for_sources=True)
    export_authors(for_sources=True)
    export_packages(for_sources=True)  # relies on full dump to be available to measure file sizes


@keep_directory
def clear_bundles():
    curdir = os.getcwd()
    try:
        os.chdir(BUNDLE_PATH)
    except FileNotFoundError:
        return
    for f in os.listdir('.'):
        if os.path.isfile(f):
            os.remove(f)
        else:
            rmtree(f)
    os.chdir(curdir)


if __name__ == '__main__':
    purged = False

    def purge():
        global purged
        if purged:
            return
        else:
            purged = True
            clear_bundles()
            zip_packages()
    # we've been experiencing many issues with strange books appearing in the toc. i believe this line should solve that
    model.library.rebuild_toc()
    action = sys.argv[1] if len(sys.argv) > 1 else None
    index_title = sys.argv[2] if len(sys.argv) > 2 else None
    if action == "export_all":
        export_all()
    elif action == "export_all_skip_existing":
        export_all(skip_existing=True)
    elif action == "export_text":
        if not index_title:
            print("To export_index, please provide index title")
        else:
            export_text(index_title, update=True)
    elif action == "export_updated":
        export_updated()
    elif action == "purge_cloudflare":  # purge general toc and last_updated files
        if USE_CLOUDFLARE:
            purge()
            purge_cloudflare_cache([])
        else:
            print("not using cloudflare")
    elif action == "export_toc":
        export_toc()
        export_topic_toc()
        if USE_CLOUDFLARE:
            purge_cloudflare_cache([])
    elif action == "export_hebrew_categories":
        export_hebrew_categories()
    elif action == "export_calendar":
        export_calendar()
    elif action == "export_authors":
        export_authors()
    elif action == "export_base_files_to_sources":
        export_base_files_to_sources()
    elif action == "export_packages":
        export_packages()
    elif action == "write_last_updated":  # for updating package infor
        write_last_updated([], True)
    purge()
    try:
        url = os.environ['SLACK_URL']
    except KeyError:
        print('slack url not configured')
        sys.exit(0)
    timestamp = datetime.fromtimestamp(os.stat(f'{EXPORT_PATH}/last_updated.json').st_mtime).ctime()
    alert_slack(f'Mobile export complete. Timestamp on `last_updated.json` is {timestamp}', ':file_folder')


