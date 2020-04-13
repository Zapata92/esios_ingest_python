# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 17:04:53 2019

@author: jzapa
"""
from urllib.error import HTTPError
import datetime
import html
import json
import pandas as pd
import re
import urllib.request


class EsiosHook():
    """
    Interact with Esios API
    :param token: personal authentication to use esios api
    :type token: str
    :param base_url: url to connect to esios api
    :type: str
    """

    def __init__(
        self,
        token,
        base_url
    ):
        self.token = token
        self.base_url = base_url

    def _get_headers_(self):
        """
        Create the specific headers to connect with esios API
        output:
            :param headers: specific headers to connect with esios Api,
                            including authorization token
            :type: dict
        """
        headers = dict()
        headers["Accept"] = ("application/json; "
                             "application/vnd.esios-api-v1+json")
        headers["Content-Type"] = "application/json"
        headers["Host"] = "api.esios.ree.es"
        headers['Authorization'] = 'Token token=\"' + self.token + '\"'
        headers["Cookie"] = ""
        return headers

    def _get_conn_(self, indicator=None,
                   start_date_esios=None,
                   end_date_esios=None):
        """
        Create connection object to interact with Esios Api, its possible to use
        indicators or not if what you want is the indicators info table
        input:
            :param indicator: indicator id which is downloaded the data
            :type str
            :start_date_esios: start date from wich data is downloaded
            :type str: format %Y-%m-%dT%H:%M:%S
            :end_date_esios: end date until wich data is downloaded
            :type str: format %Y-%m-%dT%H:%M:%S
        output:
            :param req: request urllib object
            :type urllib.Object
        """
        base_url = self.base_url
        if indicator:
            if not start_date_esios or not end_date_esios:
                raise ValueError(
                    "If use indicator id, you'll need to use some dates")
            else:
                try:
                    datetime.datetime.strptime(
                        start_date_esios, '%Y-%m-%dT%H:%M:%S')
                    datetime.datetime.strptime(
                        end_date_esios, '%Y-%m-%dT%H:%M:%S')
                    if start_date_esios >= end_date_esios:
                        raise ValueError(
                            ("end_date_esios must be upper than "
                             "start_date_esios {} > {}"
                             .format(start_date_esios, end_date_esios)))
                    else:
                        base_url = "{0}/{1}?start_date={2}&end_date={3}".format(
                            base_url,
                            str(indicator),
                            start_date_esios,
                            end_date_esios)
                except ValueError:
                    raise ValueError(
                        "Incorrect date format, "
                        "should be yyyy-mm-dd'T'hh:mm:ss")
        else:
            pass
        req = urllib.request.Request(
            base_url, headers=self._get_headers_())
        return req

    def check_and_run(self, indicator=None,
                      start_date_esios=None,
                      end_date_esios=None):
        """
        Execute url request to get data from esios api
        input:
            :param indicator: indicator id which is downloaded the data
            :type str
            :start_date_esios: start date from wich data is downloaded
            :type str: format %Y-%m-%dT%H:%M:%S
            :end_date_esios: end date until wich data is downloaded
            :type str: format %Y-%m-%dT%H:%M:%S
        output:
            :param result: data result of urllib request
            :type json 
        """    
        req = self._get_conn_(indicator, start_date_esios, end_date_esios)
        try:
            response = urllib.request.urlopen(req)
        except HTTPError as error:
            raise error
        except Exception:
            print("UnControlled Error")
            raise
        try:
            json_data = response.read().decode('utf-8')
        except:
            json_data = response.read().decode('utf-8')
        result = json.loads(json_data)
        return result