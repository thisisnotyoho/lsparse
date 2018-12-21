import bs4
import pandas as pa

class lsparse:
    def __init__(self, filename):
        with open(filename) as fd:
            self.raw = fd.read()
        self.soup = bs4.BeautifulSoup(self.raw, 'lxml')
        self.data = {}
        self.data['game'] = self.soup.run.gamename.string
        self.data['category'] = self.soup.run.categoryname.string
        self.data['offset'] = self.soup.offset.string
        self.data['count'] = self.soup.run.attemptcount.string
        self.attempts = {}
        for x in self.soup.attempthistory.find_all('attempt'):
            aid = x['id']
            self.attempts[aid] = {}
            attempt = self.attempts[aid]
            attempt['started'] = x['started']
            attempt['ended'] = x['ended']
            if x.find('realtime') is not None:
                attempt['realtime'] = x.realtime.string
            if x.find('pausetime') is not None:
                attempt['pausetime'] = x.pausetime.string
        self.splits = {}
        for x in self.soup.segments.find_all('segment'):
            name = x.find('name').string
            self.splits[name] = {}
            self.splits[name]['gold'] = x.bestsegmenttime.realtime.string
            self.splits[name]['pb_split'] = x.splittime.realtime.string
            for y in x.segmenthistory.find_all('time'):
                if y['id'] in self.attempts:
                    if y.find('realtime') is not None:
                        self.attempts[y['id']][name] = y.realtime.string
                else:
                    #print(name + " ID:" + y['id'] + " is not in attempt data!")
                    None
        self.splitsdf = pa.DataFrame(self.splits)
        self.attemptsdf = pa.DataFrame.from_dict(self.attempts,orient='index')
        self.attemptsdf.index = pa.to_numeric(self.attemptsdf.index)
        self.attemptsdf.index.name = 'id'
        tmp = lambda x : pa.to_datetime(x) - pa.to_timedelta(5,unit='h')
        self.attemptsdf['started'] = self.attemptsdf['started'].apply(tmp)
        self.attemptsdf['ended'] = self.attemptsdf['ended'].apply(tmp)
        tmp = lambda x : pa.to_timedelta(x, unit='s', errors='coerce')
        keys = list(self.splits)
        for key in self.splits:
            self.attemptsdf[key] = self.attemptsdf[key].apply(tmp)
            self.splitsdf[key] = self.splitsdf[key].apply(tmp)
        tmp = self.splitsdf.loc['pb_split'].diff()
        tmp.name = 'pb_segment'
        tmp[0] = self.splitsdf.loc['pb_split'][0]
        self.splitsdf = self.splitsdf.append(tmp)
        self.splitsdf = self.splitsdf.T
        self.splitsdf = self.splitsdf[['gold','pb_segment','pb_split']]



        
