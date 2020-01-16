#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 11 18:53:43 2020

@author: mavroudo
"""
def readTxt():
    data=[]
    with open("crawler.txt","r") as file:
        aline=[]
        for line in file:
            x=line.split("$$!")
            aline.append(x[0])
            aline.append(x[1])
            elements=[]
            try:
                for element in x[2].split(","):
                    words=element.split("-")
                    elements.append([words[0],int(words[1])])
            except:
                pass
            aline.append(elements)
            data.append(aline)
    return data
    