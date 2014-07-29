#-*- coding: utf-8 -*-
from mrjob.job import MRJob
from bs4 import BeautifulSoup
import re
import matplotlib.pyplot as plt
import IPython
"""
Implementing PageRank atop MapReduce
-- naveed

Port of hadoop implementation http://blog.xebia.com/2011/09/27/wiki-pagerank-with-hadoop/

Utility Classes:
    LineProtocol - Used to extract <page> tags from xml
    CustomMRJob - Inherited from MRJob, makes it easy to specify input/output file paths in job runner

MRJob Classes:
    PageLinkCalculate - Parse the page and output page with initial rank and outgoing results
    RankCalculate - Claculate PageRank of each wikipage
    RankingMapper - Order pages by ranks

Put input XML file in script directory
Intermediate results from RankCalculate are stored in out/0/, out/1/ ...
Final results are stored in 'out/result/' directory
"""

link_pat = re.compile("\\[.+?\\]")  # regular expression to extract links
link_page_pat = re.compile(r'\[{,2}(.+?)([\]|#])') # extract link text before ],|,# 

DAMPING_FACTOR = 0.85

class LineProtocol(object):
    """ This custom protocol is used to feed entire <page></page> xml as a line to PageLinkCalculate mapper """
    def __init__(self):
        self.lines = ""
    def read(self, line):
        self.lines += line
        if '<page>' in line:
            self.lines = ""
        elif '</page>' in line:
            # one page is found, 
            page = BeautifulSoup(self.lines)
            self.lines= ""
            return None, unicode(page)
        return None, None
    def write(self, key, value):
        return key, value

class CustomMRJob(MRJob):
    def make_runner(self, input_path, output_dir):
        ## Override make_runner to add input_path and output_dir
        self.input_paths = [input_path]
        self.output_dir = output_dir
        return super(CustomMRJob, self).make_runner()

    def job_runner_kwargs(self):
        kwargs = super(CustomMRJob, self).job_runner_kwargs()
        kwargs['input_paths'] = self.input_paths
        kwargs['output_dir'] = self.output_dir
        return kwargs

class PageLinkCalculate(CustomMRJob): 
    """
    Parse title/text of each page
    Get the article’s name and its outgoing links.
    For each wikipage obtain the links to other pages.
    Store the page, initial rank (1.0) and outgoing links.
    """
    def mapper(self, key, value): 
        if value:
            soup = BeautifulSoup(value)
            title = soup.find('title')
            text = soup.find('text')
            if title:
                title = title.string.replace(' ','_') # spaces are underscored
            if text and text.string:
                links = re.findall(link_pat, text.string)
                for link in links:
                    other_page = self._get_wiki_page_from_link(link)
                    if not other_page: continue
                    yield title, other_page

    def reducer(self, key, values):
        page_rank = "1.0"
        yield key, page_rank + "\t" + ','.join(values)
    def input_protocol(self):
        return LineProtocol()

    def _get_wiki_page_from_link(self, link):
        grp = re.search(link_page_pat, link)
        if grp:
            link_text = grp.group(1)
            link_text = re.sub(r'\s','_',link_text) # whitespaces with underscore
            link_text = link_text.replace(',','') # remove comma
            link_text = link_text.replace("&amp;","&") # convert ampersands
            return link_text
        return None

class RankCalculate(CustomMRJob):
    """
    Calculate new PageRank
    Map each outgoing link to the page with its rank and total outgoing links.
    Store the page, new rank and outgoing links.
    This job is run multiple times to generate accurate results
    """
    def mapper(self, key, value):
        value = value.strip('"')    # remove extra quotes added by previous job
        values = value.split('\t')
        page = values[0].strip('"')
        links_and_ranks = values[1].split('\\t')
        rank = links_and_ranks[0].strip('"')
        page_rank = page + "\t" + rank
        yield page, "!"
        if len(links_and_ranks) < 2:
            return
        links = links_and_ranks[1]
        other_pages = links.split(',')

        for other_page in other_pages:
            yield other_page, page_rank + "\t" + str(len(other_pages))

        yield page, "|" + links

    def reducer(self, key, values):
        existing_wiki_page = False
        split = []
        sum_share_other_ranks = 0
        links = ""
        page_with_rank = ""
        for v in values:
            page_with_rank = v
            if page_with_rank[0] == '!':
                existing_wiki_page = True
                continue
            if page_with_rank[0] == '|':
                links = "\t" + page_with_rank[1:]
                continue
            split = page_with_rank.split('\t')
            page_rank = float(split[1])
            count_out_links = int(split[2])
            sum_share_other_ranks += (page_rank/count_out_links)
        if not existing_wiki_page:
            return
        new_rank = DAMPING_FACTOR * sum_share_other_ranks + (1-DAMPING_FACTOR)
        yield key, str(new_rank) + links

class RankingMapper(CustomMRJob):
    """
    Get the page and rank. And map the key: rank to value: page.
    """
    def mapper(self, key, value):
        rank, page = self._get_page_and_rank(key, value)
        yield float("{0:.2f}".format(float(rank))), page
    
    def _get_page_and_rank(self, key, value):
        tabbed_list = value.split('\t')
        page = tabbed_list[0].strip('"')
        rank_and_links = tabbed_list[1].strip('"')
        rank = rank_and_links.split('\\t')[0]
        return (rank, page)

def plot_graph(results):
    """ Plot Top 50 results on graph, assuming results are stored in descending order """
    fp = open(results)
    head=[fp.next() for x in xrange(50)]
    vals = [float(x.split('\t')[0]) for x in head]
    ticks = [x.split('\t')[1].strip('"') for x in head]
    plt.bar(range(50), vals, color='g')
    plt.title("PageRanks of NL Wiki (Top 50)")
    ax = plt.gca()
    ax.set_xticks(range(50))
    ax.set_xticklabels(ticks, rotation='vertical')
    plt.ylabel('Page Rank')
    plt.xlabel('Page')
    plt.tick_params(axis='both', which='major', labelsize=11)
    plt.show()

if __name__ == "__main__": 
    pl_job = PageLinkCalculate()
    with pl_job.make_runner('*.xml', 'out/0/') as runner:
        runner.run()
    rc_job = RankCalculate()
    for i in range(4):
        with rc_job.make_runner('out/%d/' % i,'out/%d/' % (i+1)) as runner:
            runner.run()
    rm_job = RankingMapper()
    with rm_job.make_runner('out/4/', 'out/result') as runner:
        runner.run()

    plot_graph('out/result/final')
