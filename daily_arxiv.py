import os
import re
import json
import arxiv
import yaml
import logging
import argparse
import datetime
import requests
from time import sleep

logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)

github_url = "https://api.github.com/search/repositories"
arxiv_url = "http://arxiv.org/"

def load_config(config_file:str) -> dict:
    '''
    config_file: input config file path
    return: a dict of configuration
    '''
    # make filters pretty
    def pretty_filters(**config) -> dict:
        keywords = dict()
        EXCAPE = '\"'
        QUOTA = '' # NO-USE
        OR = 'OR' # TODO
        def parse_filters(filters:list):
            ret = ''
            for idx in range(0,len(filters)):
                filter = filters[idx]
                if len(filter.split()) > 1:
                    ret += (EXCAPE + filter + EXCAPE)  
                else:
                    ret += (QUOTA + filter + QUOTA)   
                if idx != len(filters) - 1:
                    ret += OR
            return ret
        for k,v in config['keywords'].items():
            keywords[k] = parse_filters(v['filters'])
        return keywords
    with open(config_file,'r') as f:
        config = yaml.load(f,Loader=yaml.FullLoader) 
        config['kv'] = pretty_filters(**config)
        logging.info(f'config = {config}')
    return config 

def get_authors(authors, first_author = False):
    output = str()
    if first_author == False:
        output = ", ".join(str(author) for author in authors)
    else:
        output = authors[0]
    return output

def sort_papers(papers):
    output = dict()
    keys = list(papers.keys())
    keys.sort(reverse=True)
    for key in keys:
        output[key] = papers[key]
    return output    

def get_code_link(qword:str, enable_github_search:bool=False) -> str:
    """
    Get GitHub code link. Can be disabled for faster processing.
    """
    if not enable_github_search:
        return None
        
    try:
        # Add delay to respect rate limits
        sleep(0.5)
        query = f"{qword}"
        params = {
            "q": query,
            "sort": "stars",
            "order": "desc"
        }
        r = requests.get(github_url, params=params, timeout=5)
        if r.status_code == 403:  # Rate limited
            logging.warning("GitHub API rate limited")
            return None
        results = r.json()
        code_link = None
        if "total_count" in results and results["total_count"] > 0:
            code_link = results["items"][0]["html_url"]
        return code_link
    except Exception as e:
        logging.debug(f"GitHub search failed for {qword}: {e}")
        return None
  
def get_daily_papers(topic, query="slam", max_results=2, enable_github_search=False, fetch_all=False):
    """
    @param topic: str
    @param query: str
    @param enable_github_search: bool - whether to search for GitHub links
    @param fetch_all: bool - whether to fetch all papers (for --all flag)
    @return paper_with_code: dict
    """
    # output 
    content = dict() 
    content_to_web = dict()
    client = arxiv.Client()
    # If fetch_all is True, set max_results to a very large number to get all papers
    actual_max = 10000 if fetch_all else max_results
    search = arxiv.Search(
        query = query,
        max_results = actual_max,
        sort_by = arxiv.SortCriterion.SubmittedDate
    )

    try:
        paper_count = 0
        for result in client.results(search):
            paper_count += 1
            if paper_count % 100 == 0:
                logging.info(f"Processed {paper_count} papers...")

            paper_id            = result.get_short_id()
            paper_title         = result.title
            paper_url           = result.entry_id
            paper_abstract      = result.summary.replace("\n"," ")
            paper_authors       = get_authors(result.authors)
            paper_first_author  = get_authors(result.authors,first_author = True)
            primary_category    = result.primary_category
            publish_time        = result.published.date()
            update_time         = result.updated.date()
            comments            = result.comment

            logging.info(f"Time = {update_time} title = {paper_title} author = {paper_first_author}")

            # eg: 2108.09112v1 -> 2108.09112
            ver_pos = paper_id.find('v')
            if ver_pos == -1:
                paper_key = paper_id
            else:
                paper_key = paper_id[0:ver_pos]    
            paper_url = arxiv_url + 'abs/' + paper_key
            
            # Only search GitHub if enabled
            repo_url = None
            if enable_github_search:
                repo_url = get_code_link(paper_title, enable_github_search)
                if repo_url is None:
                    repo_url = get_code_link(paper_key, enable_github_search)
        
            if repo_url is not None:
                content[paper_key] = "|**{}**|**{}**|{} et.al.|[{}]({})|**[link]({})**|\n".format(
                       update_time,paper_title,paper_first_author,paper_key,paper_url,repo_url)
                content_to_web[paper_key] = "- {}, **{}**, {} et.al., Paper: [{}]({}), Code: **[{}]({})**".format(
                       update_time,paper_title,paper_first_author,paper_url,paper_url,repo_url,repo_url)
            else:
                content[paper_key] = "|**{}**|**{}**|{} et.al.|[{}]({})|null|\n".format(
                       update_time,paper_title,paper_first_author,paper_key,paper_url)
                content_to_web[paper_key] = "- {}, **{}**, {} et.al., Paper: [{}]({})".format(
                       update_time,paper_title,paper_first_author,paper_url,paper_url)

            # TODO: select useful comments
            comments = None
            if comments != None:
                content_to_web[paper_key] += f", {comments}\n"
            else:
                content_to_web[paper_key] += f"\n"
                
    except arxiv.UnexpectedEmptyPageError:
        # This error occurs when there are no more results to fetch
        logging.info("Reached the end of available results")
        pass
    except Exception as e:
        logging.error(f"Error fetching papers: {e}")
        pass

    logging.info(f"Total papers fetched for {topic}: {paper_count}")
    data = {topic:content}
    data_web = {topic:content_to_web}
    return data,data_web 

def update_paper_links(filename):
    '''
    weekly update paper links in json file 
    '''
    def parse_arxiv_string(s):
        parts = s.split("|")
        date = parts[1].strip()
        title = parts[2].strip()
        authors = parts[3].strip()
        arxiv_id = parts[4].strip()
        code = parts[5].strip()
        arxiv_id = re.sub(r'v\d+', '', arxiv_id)
        return date,title,authors,arxiv_id,code

    with open(filename,"r") as f:
        content = f.read()
        if not content:
            m = {}
        else:
            m = json.loads(content)
            
        json_data = m.copy() 

        for keywords,v in json_data.items():
            logging.info(f'keywords = {keywords}')
            for paper_id,contents in v.items():
                contents = str(contents)

                update_time, paper_title, paper_first_author, paper_url, code_url = parse_arxiv_string(contents)

                contents = "|{}|{}|{}|{}|{}|\n".format(update_time,paper_title,paper_first_author,paper_url,code_url)
                json_data[keywords][paper_id] = str(contents)
                logging.info(f'paper_id = {paper_id}, contents = {contents}')
                
                valid_link = False if '|null|' in contents else True
                if valid_link:
                    continue
                # Try to find code link from GitHub search
                repo_url = get_code_link(paper_title, enable_github_search=True)
                if repo_url is None:
                    repo_url = get_code_link(paper_id, enable_github_search=True)
                if repo_url is not None:
                    new_cont = contents.replace('|null|',f'|**[link]({repo_url})**|')
                    logging.info(f'ID = {paper_id}, contents = {new_cont}')
                    json_data[keywords][paper_id] = str(new_cont)
        # dump to json file
        with open(filename,"w") as f:
            json.dump(json_data,f)

def update_json_file(filename,data_dict,clear_existing=False):
    '''
    daily update json file using data_dict
    @param clear_existing: bool - whether to clear existing data (for --all flag)
    '''
    logging.info(f"update_json_file: Starting update for {filename}, clear_existing={clear_existing}")
    
    if clear_existing:
        # Clear existing data when --all flag is used
        logging.info(f"Clearing existing data in {filename}")
        m = {}
    else:
        logging.info(f"Reading existing data from {filename}")
        try:
            with open(filename,"r") as f:
                content = f.read()
                if not content:
                    m = {}
                else:
                    m = json.loads(content)
        except FileNotFoundError:
            logging.info(f"File {filename} not found, creating new")
            m = {}
            
    json_data = m.copy() 
    
    # update papers in each keywords  
    logging.info(f"Updating {len(data_dict)} topics")       
    for data in data_dict:
        for keyword in data.keys():
            papers = data[keyword]
            logging.info(f"Updating keyword '{keyword}' with {len(papers)} papers")

            if keyword in json_data.keys():
                json_data[keyword].update(papers)
            else:
                json_data[keyword] = papers

    logging.info(f"Writing updated data to {filename}")
    with open(filename,"w") as f:
        json.dump(json_data,f)
    logging.info(f"update_json_file: Completed for {filename}")
    
def json_to_md(filename,md_filename,
               task = '',
               to_web = False, 
               use_title = True, 
               use_tc = True,
               show_badge = True,
               use_b2t = True):
    """
    @param filename: str
    @param md_filename: str
    @return None
    """
    logging.info(f"json_to_md: Starting for {filename} -> {md_filename}, task={task}")
    
    def pretty_math(s:str) -> str:
        ret = ''
        match = re.search(r"\$.*\$", s)
        if match == None:
            return s
        math_start,math_end = match.span()
        space_trail = space_leading = ''
        if s[:math_start][-1] != ' ' and '*' != s[:math_start][-1]: space_trail = ' ' 
        if s[math_end:][0] != ' ' and '*' != s[math_end:][0]: space_leading = ' ' 
        ret += s[:math_start] 
        ret += f'{space_trail}${match.group()[1:-1].strip()}${space_leading}' 
        ret += s[math_end:]
        return ret
  
    DateNow = datetime.date.today()
    DateNow = str(DateNow)
    DateNow = DateNow.replace('-','.')
    
    logging.info(f"json_to_md: Reading JSON from {filename}")
    with open(filename,"r") as f:
        content = f.read()
        if not content:
            data = {}
        else:
            data = json.loads(content)
    logging.info(f"json_to_md: Loaded {len(data)} topics from JSON")

    # clean README.md if daily already exist else create it
    logging.info(f"json_to_md: Creating/clearing {md_filename}")
    with open(md_filename,"w+") as f:
        pass

    # write data into README.md
    logging.info(f"json_to_md: Writing content to {md_filename}")
    with open(md_filename,"a+") as f:

        if (use_title == True) and (to_web == True):
            f.write("---\n" + "layout: default\n" + "---\n\n")
        
        if show_badge == True:
            f.write(f"[![Contributors][contributors-shield]][contributors-url]\n")
            f.write(f"[![Forks][forks-shield]][forks-url]\n")
            f.write(f"[![Stargazers][stars-shield]][stars-url]\n")
            f.write(f"[![Issues][issues-shield]][issues-url]\n\n")    
                
        if use_title == True:
            f.write("## Updated on " + DateNow + "\n")
        else:
            f.write("> Updated on " + DateNow + "\n")
            
        f.write("> Welcome to Video Generation papers! \n\n")

        if use_tc == True:
            f.write("<details>\n")
            f.write("  <summary>Table of Contents</summary>\n")
            f.write("  <ol>\n")
            for keyword in data.keys():
                day_content = data[keyword]
                if not day_content:
                    continue
                kw = keyword.replace(' ','-')      
                f.write(f"    <li><a href=#{kw.lower()}>{keyword}</a></li>\n")
            f.write("  </ol>\n")
            f.write("</details>\n\n")
        
        for keyword in data.keys():
            day_content = data[keyword]
            if not day_content:
                continue
            # the head of each part
            f.write(f"## {keyword}\n\n")

            if use_title == True :
                if to_web == False:
                    f.write("|Publish Date|Title|Authors|PDF|Code|\n" + "|---|---|---|---|---|\n")
                else:
                    f.write("| Publish Date | Title | Authors | PDF | Code |\n")
                    f.write("|:---------|:-----------------------|:---------|:------|:------|\n")

            # sort papers by date
            logging.info(f"json_to_md: Sorting {len(day_content)} papers for {keyword}")
            day_content = sort_papers(day_content)
        
            logging.info(f"json_to_md: Writing papers for {keyword}")
            for _,v in day_content.items():
                if v is not None:
                    f.write(pretty_math(v)) # make latex pretty

            f.write(f"\n")
            
            #Add: back to top
            if use_b2t:
                top_info = f"#Updated on {DateNow}"
                top_info = top_info.replace(' ','-').replace('.','')
                f.write(f"<p align=right>(<a href={top_info.lower()}>back to top</a>)</p>\n\n")
            
        if show_badge == True:
            # we don't like long string, break it!
            f.write((f"[contributors-shield]: https://img.shields.io/github/"
                     f"contributors/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[contributors-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/graphs/contributors\n"))
            f.write((f"[forks-shield]: https://img.shields.io/github/forks/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[forks-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/network/members\n"))
            f.write((f"[stars-shield]: https://img.shields.io/github/stars/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[stars-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/stargazers\n"))
            f.write((f"[issues-shield]: https://img.shields.io/github/issues/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[issues-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/issues\n\n"))
    
    logging.info(f"json_to_md: Completed writing to {md_filename}")            
    logging.info(f"{task} finished")        

def demo(**config):
    # TODO: use config
    data_collector = []
    data_collector_web= []
    
    keywords = config['kv']
    max_results = config['max_results']
    publish_readme = config['publish_readme']
    publish_gitpage = config['publish_gitpage']
    publish_wechat = config['publish_wechat']
    show_badge = config['show_badge']
    enable_github_search = config.get('enable_github_search', False)
    fetch_all = config.get('fetch_all', False)

    b_update = config['update_paper_links']
    logging.info(f'Update Paper Link = {b_update}')
    if config['update_paper_links'] == False:
        if fetch_all:
            logging.info(f"FETCH ALL mode: Getting ALL papers (this may take a while)")
        else:
            logging.info(f"GET daily papers begin")
        for topic, keyword in keywords.items():
            logging.info(f"Keyword: {topic}")
            data, data_web = get_daily_papers(topic, query = keyword,
                                            max_results = max_results,
                                            enable_github_search = enable_github_search,
                                            fetch_all = fetch_all)
            data_collector.append(data)
            data_collector_web.append(data_web)
            print("\n")
        if fetch_all:
            logging.info(f"FETCH ALL mode: Completed fetching all papers")
        else:
            logging.info(f"GET daily papers end")

    # 1. update README.md file
    if publish_readme:
        logging.info(f"Starting README.md update...")
        json_file = config['json_readme_path']
        md_file   = config['md_readme_path']
        logging.info(f"JSON file: {json_file}, MD file: {md_file}")
        # update paper links
        if config['update_paper_links']:
            logging.info(f"Updating paper links in {json_file}")
            update_paper_links(json_file)
        else:    
            # update json data
            logging.info(f"Updating JSON file {json_file} with {len(data_collector)} topics")
            update_json_file(json_file,data_collector,clear_existing=fetch_all)
            logging.info(f"JSON file update completed")
        # json data to markdown
        logging.info(f"Converting JSON to Markdown: {json_file} -> {md_file}")
        json_to_md(json_file,md_file, task ='Update Readme', \
            show_badge = show_badge)
        logging.info(f"README.md update completed")

    # 2. update docs/index.md file (to gitpage)
    if publish_gitpage:
        logging.info(f"Starting GitPage update...")
        json_file = config['json_gitpage_path']
        md_file   = config['md_gitpage_path']
        logging.info(f"GitPage JSON file: {json_file}, MD file: {md_file}")
        # TODO: duplicated update paper links!!!
        if config['update_paper_links']:
            logging.info(f"Updating paper links in {json_file}")
            update_paper_links(json_file)
        else:    
            logging.info(f"Updating GitPage JSON file with {len(data_collector)} topics")
            update_json_file(json_file,data_collector,clear_existing=fetch_all)
            logging.info(f"GitPage JSON file update completed")
        logging.info(f"Converting GitPage JSON to Markdown")
        json_to_md(json_file, md_file, task ='Update GitPage', \
            to_web = True, show_badge = show_badge, \
            use_tc=False, use_b2t=False)
        logging.info(f"GitPage update completed")

    # 3. Update docs/wechat.md file
    if publish_wechat:
        logging.info(f"Starting WeChat update...")
        json_file = config['json_wechat_path']
        md_file   = config['md_wechat_path']
        logging.info(f"WeChat JSON file: {json_file}, MD file: {md_file}")
        # TODO: duplicated update paper links!!!
        if config['update_paper_links']:
            logging.info(f"Updating paper links in {json_file}")
            update_paper_links(json_file)
        else:    
            logging.info(f"Updating WeChat JSON file with {len(data_collector_web)} topics")
            update_json_file(json_file, data_collector_web,clear_existing=fetch_all)
            logging.info(f"WeChat JSON file update completed")
        logging.info(f"Converting WeChat JSON to Markdown")
        json_to_md(json_file, md_file, task ='Update Wechat', \
            to_web=False, use_title= False, show_badge = show_badge)
        logging.info(f"WeChat update completed")
    
    logging.info(f"========== ALL UPDATES COMPLETED SUCCESSFULLY ==========")
    if fetch_all:
        logging.info(f"FETCH ALL mode: All papers have been fetched and files have been updated")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_path',type=str, default='config.yaml',
                            help='configuration file path')
    parser.add_argument('--update_paper_links', default=False,
                        action="store_true",help='whether to update paper links etc.')
    parser.add_argument('--enable_github_search', default=False,
                        action="store_true",help='whether to search for GitHub links')
    parser.add_argument('--all', default=False,
                        action="store_true",help='refresh all records by deleting existing data and re-fetching all papers')                   
    args = parser.parse_args()
    config = load_config(args.config_path)
    config = {**config, 'update_paper_links':args.update_paper_links,
            'enable_github_search':args.enable_github_search,
            'fetch_all':args.all}
    demo(**config)