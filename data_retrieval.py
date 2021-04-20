import os

import pandas as pd
import requests
from basketball_reference_web_scraper import client
from basketball_reference_web_scraper.data import OutputType
from lxml import html
from tinydb import TinyDB, Query


class MVPIdentificationRow:
    def __init__(self, html_snippet):
        self.html = html_snippet

    @property
    def player_cell(self):
        cells = self.html.xpath('td[@data-stat="player"]')

        if len(cells) > 0:
            return cells[0]

        return None

    @property
    def slug(self):
        cell = self.player_cell
        if cell is None:
            return ''

        return cell.get('data-append-csv')

    @property
    def name(self):
        cell = self.player_cell
        if cell is None:
            return ''

        return cell.text_content()

    @property
    def season_end(self):
        cells = self.html.xpath("th[@data-stat='season']")
        if len(cells) > 0:
            return cells[0].text_content()[0:2] + cells[0].text_content()[5:7]

        return None


class BasketballDatabase:
    def __init__(self, path=None):
        if path is not None:
            self.db = TinyDB(path)

    def feed_season_stats_json_to_db(self, season_stats, end_year):
        for row in season_stats:
            row["season_end_year"] = end_year
            self.db.insert(row)

    @property
    def rows_query_mvp(self):
        return """
            //table[@id="mvp_NBA"]
            /tbody
            /tr
        """

    def get_mvp_winners(self):
        resp = requests.get("https://www.basketball-reference.com/awards/mvp.html")
        html_content = html.fromstring(resp.content)
        winners = []
        for row in html_content.xpath(self.rows_query_mvp):
            id_row = MVPIdentificationRow(row)

            winners.append({"slug": id_row.slug, "name": id_row.name, "season": id_row.season_end})
        with open("mvp_winners.csv", "w") as mvp_file:
            df = pd.DataFrame(winners)
            df.to_csv(mvp_file, index=False)

    @staticmethod
    def combine_all_csv_files(parent_directory_path, year_start, year_end, delete_on_complete=False,
                              file_name="adv_combined_stats.csv"):
        file_names = [os.path.join(parent_directory_path, "stats_{}.csv".format(i)) for i in
                      range(year_start, year_end + 1)]
        combined_csv = pd.concat([pd.read_csv(f) for f in file_names])
        combined_csv.to_csv(os.path.join(parent_directory_path, file_name), index=False)
        if delete_on_complete:
            for f in file_names:
                os.remove(f)

    @staticmethod
    def create_player_totals_csv(year_start, year_end, advanced=False):
        for i in range(year_start, year_end + 1):
            if advanced:
                if not os.path.exists("adv_total_stats"):
                    os.makedirs("adv_total_stats")
                client.players_advanced_season_totals(season_end_year=i, output_type=OutputType.CSV,
                                                      output_file_path="adv_total_stats/stats_{}.csv".format(i))
            else:
                if not os.path.exists("total_stats"):
                    os.makedirs("total_stats")
                client.players_season_totals(season_end_year=i, output_type=OutputType.CSV,
                                             output_file_path="total_stats/stats_{}.csv".format(i))
            # self.feed_season_stats_to_db(stats, i)
            print("Exported stats for the year {}. {} percent completed".format(i, 100 * ((i - year_start + 1) / (
                    year_end - year_start + 1))))

    def get_player_stats_by_name(self, name):
        q = Query()
        results = self.db.search(q.name == name)
        return results


def export_player_totals_csv():
    bbdb.create_player_totals_csv(1956, 2020, advanced=False)
    bbdb.combine_all_csv_files("total_stats", 1956, 2020)


def export_player_totals_adv_csv():
    bbdb.create_player_totals_csv(1956, 2020, advanced=True)
    bbdb.combine_all_csv_files("adv_total_stats", 1956, 2020)


def get_mvp():
    bbdb.get_mvp_winners()


if __name__ == '__main__':
    bbdb = BasketballDatabase()
    get_mvp()
