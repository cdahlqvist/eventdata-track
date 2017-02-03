import json
import random
import math
import datetime
import calendar
import gzip
import re
from parameter_sources.weightedarray import WeightedArray
from parameter_sources.timeutils import TimestampStructGenerator

class Agent:
    def __init__(self):
        self._agents = WeightedArray('parameter_sources/data/agents.json.gz')

        with gzip.open('parameter_sources/data/agents_name_lookup.json.gz', 'rt') as data_file:    
            self._agents_name_lookup = json.load(data_file)

        with gzip.open('parameter_sources/data/agents_os_lookup.json.gz', 'rt') as data_file:    
            self._agents_os_lookup = json.load(data_file)

        with gzip.open('parameter_sources/data/agents_os_name_lookup.json.gz', 'rt') as data_file:    
            self._agents_os_name_lookup = json.load(data_file)

    def add_fields(self, event):
        agent = self._agents.get_random()

        event['agent'] = "\"{}\"".format(agent[0])
        event['useragent'] = {}
        event['useragent']['os'] = self._agents_os_lookup[agent[1]]
        event['useragent']['os_name'] = self._agents_os_name_lookup[agent[2]]
        event['useragent']['name'] = self._agents_name_lookup[agent[3]]

class ClientIp:
    def __init__(self):
        self._rare_clientip_probability = 0.269736965199
        self._clientips = WeightedArray('parameter_sources/data/clientips.json.gz')
        self._rare_clientips = WeightedArray('parameter_sources/data/rare_clientips.json.gz')

        with gzip.open('parameter_sources/data/clientips_country_lookup.json.gz', 'rt') as data_file:    
            self._clientips_country_lookup = json.load(data_file)

        with gzip.open('parameter_sources/data/clientips_location_lookup.json.gz', 'rt') as data_file:    
            self._clientips_location_lookup = json.load(data_file)

    def add_fields(self, event):
        p = random.random()
        event['geoip'] = {}

        if p < self._rare_clientip_probability:
            data = self._rare_clientips.get_random()
            event['clientip'] = self.__fill_out_ip_prefix(data[0])
        else:
            data = self._clientips.get_random()
            event['clientip'] = data[0]

        event['geoip']['country_name'] = self._clientips_country_lookup[data[1]]
        event['geoip']['location'] = self._clientips_location_lookup[data[2]]

    def __fill_out_ip_prefix(self, ip_prefix):
        rnd1 = random.random()
        v1 = rnd1 * ( 1 - rnd1) * 255 * 4
        k1 = math.floor(v1)
        rnd2 = random.random()
        v2 = rnd2 * ( 1 - rnd2) * 255 * 4
        k2 = math.floor(v2)

        return "{}.{}.{}".format(ip_prefix, k1, k2)

class Referrer:
    def __init__(self):
        self._referrers = WeightedArray('parameter_sources/data/referrers.json.gz')

        with gzip.open('parameter_sources/data/referrers_url_base_lookup.json.gz', 'rt') as data_file:    
            self._referrers_url_base_lookup = json.load(data_file)

    def add_fields(self, event):
        data = self._referrers.get_random()

        event['referrer'] = "\"{}{}\"".format(self._referrers_url_base_lookup[data[0]], data[1])

class Request:
    def __init__(self):
        self._requests = WeightedArray('parameter_sources/data/requests.json.gz')

        with gzip.open('parameter_sources/data/requests_url_base_lookup.json.gz', 'rt') as data_file:    
            self._requests_url_base_lookup = json.load(data_file)

    def add_fields(self, event):
        data = self._requests.get_random()

        event['request'] = "{}{}".format(self._requests_url_base_lookup[data[0]], data[1])
        event['bytes'] = data[2]
        event['verb'] = data[3]
        event['response'] = data[4]
        event['httpversion'] = data[5]

class RandomEvent:
    def __init__(self, params):
        self._agent = Agent()
        self._clientip = ClientIp()
        self._referrer = Referrer()
        self._request = Request()

        self._index = 'elasticlogs'
        self._index_pattern = False
        if 'index' in params.keys():
            index = re.sub(r'{{\s*yyyy\s*}}', '{ts[yyyy]}', params['index'], flags=re.IGNORECASE)
            index = re.sub(r'{{\s*yy\s*}}', '{ts[yy]}', index, flags=re.IGNORECASE)
            index = re.sub(r'{{\s*mm\s*}}', '{ts[mm]}', index, flags=re.IGNORECASE)
            index = re.sub(r'{{\s*dd\s*}}', '{ts[dd]}', index, flags=re.IGNORECASE)
            index = re.sub(r'{{\s*hh\s*}}', '{ts[hh]}', index, flags=re.IGNORECASE)
            self._index = index
            self._index_pattern = True

        self._type = 'logs'
        if 'type'  in params.keys():
            self._type = params['type']

        if 'starting_point' in params.keys():
            sp = params['starting_point']
        else:
            sp ="now"

        if 'end_point' in params.keys():
            ep = params['end_point']
            self._timestamp_generator = TimestampStructGenerator.Interval(sp, ep)
        else:
            if 'acceleration_factor' in params.keys():
                af = float(params['acceleration_factor'])
                self._timestamp_generator = TimestampStructGenerator.StartingPoint(sp, af)
            else:
                self._timestamp_generator = TimestampStructGenerator.StartingPoint(sp)

        self._delete_fields = []
        if 'delete_fields' in params.keys():
            for d in params['delete_fields']:
                self._delete_fields.append(d.split('.'))

    def generate_event(self):
        event = {}
        self._agent.add_fields(event)
        self._clientip.add_fields(event)
        self._referrer.add_fields(event)
        self._request.add_fields(event)

        timestruct = self._timestamp_generator.generate_timestamp_struct()
        event['@timestamp'] = timestruct['iso']

        if 'message' not in self._delete_fields:
            event['message'] = self.__generate_message_field(event)

        index_name = self.__generate_index_pattern(timestruct)

        self.__delete_requested_fields(event)

        return event, index_name, self._type

    def __generate_index_pattern(self, timestruct):
        if self._index_pattern:
            return self._index.format(ts=timestruct)
        else:
            return self._index








#    def __string_to_datetime(self, str):
#        c = re.split('\D+', str)
#
#        if len(c) >= 6 and len(c[0]) == 4:
#        	return datetime.datetime.strptime('{} {} {} {} {} {} UTC'.format(c[0], c[1], c[2], c[3], c[4], c[5]), "%Y %m %d %H %M %S %Z")
#        elif len(c) >= 6 and len(c[0]) == 2:
#            return datetime.datetime.strptime('{} {} {} {} {} {} UTC'.format(c[0], c[1], c[2], c[3], c[4], c[5]), "%y %m %d %H %M %S %Z")
#        elif len(c) >= 3 and len(c[0]) == 4:
#            return datetime.datetime.strptime('{} {} {} UTC'.format(c[0], c[1], c[2]), "%Y %m %d %Z")
#        else:
#            return datetime.datetime.strptime('{} {} {} UTC'.format(c[0], c[1], c[2]), "%y %m %d %Z")

#    def __parse_date_range(self, str):
#        dates = str.split(',')

#        if len(dates) == 1:
#            now = datetime.datetime.utcnow()
#            dt1 = self.__string_to_datetime(dates[0])

#            if now > dt1:
#            	return [dt1, (now - dt1).total_seconds()]
#            else:
#                return [now, (dt1 - now).total_seconds()]

#        else:
#            dt1 = self.__string_to_datetime(dates[0])
#            dt2 = self.__string_to_datetime(dates[1])

#            if dt2 > dt1:
#            	return [dt1, (dt2 - dt1).total_seconds()]
#            else:
#                return [dt2, (dt1 - dt2).total_seconds()]
    
#    def __get_event_datetime(self):	
#        if self._time_interval == 'now':
#        	return datetime.datetime.utcnow()
#        else:
#            r = random.random() * self._time_interval[1]
#            dt = self._time_interval[0] + datetime.timedelta(seconds=r)
#            return dt

#    def __generate_time_struct(self, date_time):
#        ts = {}
#        ts['iso'] = "{}Z".format(date_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
#        ts['yyyy'] = ts['iso'][:4]
#        ts['yy'] = ts['iso'][2:4]
#        ts['mm'] = ts['iso'][5:7]
#        ts['dd'] = ts['iso'][8:10]
#        ts['hh'] = ts['iso'][11:13]

#        return ts

    def __generate_message_field(self, event):
        return '{} - - [{}] "{} {} HTTP/{}" {} {} "-" "{}"'.format(event['clientip'], event['@timestamp'], event['verb'], event['request'], event['httpversion'], event['response'], event['bytes'], event['agent'])

    def __delete_requested_fields(self, event):
        for d in self._delete_fields:
            if len(d) == 1 and d[0] in event.keys():
                del event[d[0]]
            elif len(d) == 2 and d[0] in event.keys() and d[1] in event[d[0]].keys():
                del event[d[0]][d[1]]
