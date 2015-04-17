import json
import sys
import re
import unicodedata
import heapq
from collections import defaultdict
import time
import random

STOPS = set(['is',])

def normalize(s):
  return unicodedata.normalize('NFKD', (s.lower())).encode('ascii','ignore').strip()

def loadJson(filename,normedFields):
  f = open(filename,"r")
  ret = []

  try:
    i = 0
    for l in f.readlines():
      i+=1
      p = json.loads(l)
      for k in normedFields:
        if k not in p:
          sys.stderr.write("Error:%s\n" %p)
          raise Exception("no %s field in '%s' linenum:%d" % (k,filename,i))
        p[k] = normalize(p[k])
      ret.append(p)
  except Exception:
    raise
  finally:
    f.close()

  return ret

def tokenizeList(s):
  #s = re.sub(u'\xe9',"e",s.lower())

  tokens = [s.strip() for s in s.split(' ') if len(s)]

  return tokens

def tokenize(s):
  return set(tokenizeList(s))

def jaccard(a, b):
    c = a.intersection(b)
    return float(len(c)) / (len(a) + len(b) - len(c))

def shingle(s, k):
    """Generate k-length shingles of string s."""
    k = min(len(s), k)
    for i in range(len(s) - k + 1):
        yield s[i:i+k]

def st_time(func):
    """
        st decorator to calculate the total time of a func
    """

    def st_func(*args, **keyArgs):
        t1 = time.time()
        r = func(*args, **keyArgs)
        t2 = time.time()
        print "Function=%s, Time=%.2f sec" % (func.__name__, t2 - t1)
        return r

    return st_func

def tokenizeProduct(s):
  s = re.sub(u'[ )()+/_\\-+]+'," ",s)

  #s = re.sub(u'\xe9',"e",s.lower())

  tokens = [s.strip() for s in s.split(' ') if len(s)]

  #if len(tokens)>3:
  #  tokens[2] += "".join(tokens[2:])

  return tokens

def mixproducts(pt):
    if len(pt) == 0:
      return []
    elif len(pt) == 1:
      return [pt[0]]
    elif len(pt) == 2:
        return [pt[0],pt[1],pt[0]+pt[1]]
    else:
        s = mixproducts(pt[1:])
        ret = [pt[0]] + s + [pt[0] + _ for _ in s ]
        return ret


@st_time
def loadData():
  products = loadJson("products.txt",['manufacturer','product_name','model'])
  listings = loadJson("listings.txt",['title','manufacturer'])
  return (products,listings)

@st_time
def indexData(products,listings):
  for p in products:
    product_tokens = set(mixproducts(tokenizeProduct(p['product_name'])))
    model_permutations = set(mixproducts(tokenizeProduct(p["model"])))
    t2 = tokenize(p["manufacturer"] )
    #for i in t1:
    #  for j in t1:
    #    if i != j:
    #      t2.add(i + j)

    p['tokens'] = t2.union(product_tokens).union(model_permutations).difference(STOPS)
  

  for l in listings:
    l['title'] = re.sub(u'[ )()+/_\\-+]+'," ",l['title'])
    l['tokens'] = tokenize(l['title'] + " " + l["manufacturer"] ).difference(STOPS)

  ptokens = set()
  ltokens = set()

  for p in products:
    ptokens.update(p['tokens'])

  for p in listings:
    ltokens.update(p['tokens'])

  ptokens.intersection_update(ltokens)

  for p in products:
    p['tokens'].intersection_update(ptokens)

  #for l in listings:
  #  l['tokens'].intersection_update(ptokens)

  revIdx = defaultdict(list)

  for i,p in enumerate(products):
    for t in p['tokens']:
      revIdx[t].append(i)

  return (revIdx,)

@st_time
def clusterData(products,listings):
  dictionary = Cluster(.1, 10)
  for p in products:
    s = p["product_name"] + " " + p["model"] + " " + p["manufacturer"]
    s = re.sub(u'[ )()]+',"",s)
    s = re.sub(u'[/_-]+',"",s)

    dictionary.add(frozenset(shingle(s, 4)), p['product_name'])

  return (dictionary,)

def heuristicScore(l,p,jaccard):
  # Max tagged len
  tokens = p['tokens']
  maxl = 0
  title = l['title'].split(" ")
  for i in range(len(title)):
    cl = 0 
    for j in range(i,len(title)):
      if title[j] in tokens:
        cl += 1
      else:
        break
    if cl > maxl:
      maxl = cl
  return maxl + jaccard

@st_time
def scoreData(products,listinigs,revIdx):
  for l in listings:
    candidates = []
    for c in l['tokens']:
      #print revIdx
      candidates+=revIdx[c]

    candidates = list(set(candidates))

    if len(candidates) == 0:
      continue

    jaccards = [(jaccard(l['tokens'],products[c]['tokens']),i) for i,c in enumerate(candidates)]

    maxcands = heapq.nlargest(3,jaccards,key=lambda x:x[0])

    if maxcands[0][0]<=0.05:
      continue

    heursitics = [ ( heuristicScore(l,products[candidates[c[1]]],c[0]),products[candidates[c[1]]] ) for c in maxcands ] 

    heursitics = sorted(heursitics,key = lambda x:-x[0])

    bestMatch = heursitics[0][1]
    if 'listings' not in bestMatch:
      bestMatch['listings'] = [ l ]
    else:
      bestMatch['listings'].append(l)


@st_time
def writeResult(products):
  def cleanListing(l):
    return {
      "title": l["title"],
      "manufacturer": l["manufacturer"],
      "currency": l["currency"],
      "price": l["price"]
    }
  f = open("result.txt","w")
  for p in products:
    listings = []
    if "listings" in p:
      listings = [cleanListing(l) for l in p["listings"] ]
    r = { "product_name": p['product_name'] , "listings" : listings}
    f.write(json.dumps(r) + "\n")
  f.close()

(products,listings) = loadData()
(revIdx,) = indexData(products,listings)
scoreData(products,listings,revIdx)
writeResult(products)
