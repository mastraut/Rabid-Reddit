import pyspark as ps
import numpy as np
import pandas as pd
import praw
from pprint import pprint

get_ipython().magic(u'matplotlib inline')

sc = ps.SparkContext('local[2]')

# Scrape Reddit for submissions, and store all comment data
class RabidReddit:
    """Class object for api calls to subreddit"""
    # Initialize PRAW
    # Praw has a build in rate limiter. 1 API call every 2 seconds.
    user_agent = ("no-one")
    r = praw.Reddit(user_agent=user_agent)

    def __init__(self):
        """root_only will return only top-level comments"""
        self.submissions = None

    def grab_submissions(self):
        """store a generator for submissions that match the url"""
        subreddit = "explainitlikeim5"
        # praw isn't handling urls correctly, use url:'url' instead of just 'url'
        first = True   # bool switch to not check last id 
        counter = 0    # to limit for testing practices
        while True and counter < 4:
            error_count = 0
            try:
#                 submission_generator = self.r.search('url:%s' % self.url, subreddit=subreddit) # to search specific
                if first:
                    submission_generator = self.r.get_content("https://www.reddit.com/r/%s" %subreddit, limit=0)
                    self.submissions = [sub_obj for sub_obj in submission_generator]
                    first = False
                else:
                    r = praw.Reddit(user_agent=user_agent)
                    submission_generator = r.get_content("https://www.reddit.com/r/%s" %subreddit,                                                          limit=0, params = {'before': str(submissions[-1].id)})
                    self.submissions.extend([sub_obj for sub_obj in submission_generator])
                    counter += 1
            except praw.errors.APIException:
                print 'API Exception caught... trying again'
                error_count += 1
                continue
            except praw.errors.ClientException:
                print 'Client Exception caught... trying again'
                error_count +=1
                continue
            if error_count > 5:
                print '5 errors found, moving on'   
                break

    def parse_submission_data(self, submission):
        """Returns a tuple of relevant submission data"""
        subreddit_name = str(submission.subreddit)
        subreddit_id = submission.subreddit_id
        subscriber_count = submission.subreddit.subscribers # number of redditers subscribed to subreddit
        sub_id = submission.id
        title = submission.title  # submission title
        author = submission.author # account name of poster, null if promotional link
        creation_date = submission.created_utc # date utc
        comment_count = submission.num_comments
        score = submission.score # The net-score of the link.                    
        nsfw = submission.over_18 # Subreddit marked NSFW
        
        return (subreddit_id, 
                subreddit_name, 
                subscriber_count, 
                sub_id, 
                title, 
                author, 
                creation_date, 
                comment_count,  
                score, 
                nsfw)

    def build_submission_table(self):
        """Returns a list of all submission data for the patch"""
        submission_table = []
        for submission in self.submissions:
            data = self.parse_submission_data(submission)
            submission_table.append(data)
        return submission_table

    def get_comments(self, submission):
        """
        Replaces MoreComment objects with Comment objects.
        Returns list of objects."""
#         if root_only:
        # Iterate through root comments replacing MoreComment objects
        # API is the rate limiter, not loop time
        while praw.objects.MoreComments in submission.comments:
            for i, comment in enumerate(submission.comments):
                if type(comment) == praw.objects.MoreComments:
                    submission.comments.extend(submission.comments.pop[i].comments())
        return submission.comments
#         else:
#             while True:
#                 error_count = 0
#                 try:
#                     submission.replace_more_comments(limit=None)
#                     commentobjs = praw.helpers.flatten_tree(submission.comments)
#                     return commentobjs
#                 except praw.errors.APIException:
#                     print 'API Exception caught... trying again'
#                     error_count += 1
#                     continue
#                 except praw.errors.ClientException:
#                     print 'Client Exception caught... trying again'
#                     error_count +=1
#                     continue
#                 if error_count > 5:
#                     print '5 errors found, moving on'
#                     break


    def parse_comment_data(self, comment):
        """Returns a tuple of relevant comment data"""
        comment_id = comment.id
        parent_id = comment.parent_id
        submission_id = comment.submission.id
        subreddit_id = str(comment.subreddit)
        creation_date = comment.created_utc
        banned_by = comment.banned_by
        score = comment.score
        gilded = comment.gilded # number of times comment received reddit gold
        likes = comment.likes
        controversiality = comment.controversiality
        text = comment.body
        return (
            comment_id,
            parent_id,
            submission_id,
            subreddit_id,
            creation_date,
            banned_by,
            score,
            gilded,
            likes,
            controversiality,
            text)

    def build_comment_table(self):
        comment_table = []
        for submission in self.submissions:
            comments = self.get_comments(submission)
            for comment in comments:
                data = self.parse_comment_data(comment)
                comment_table.append(data)
        return comment_table

    def collect_all(self):
        """collect all data and return submission and comment table"""
        self.grab_submissions()
        submission_table = self.build_submission_table()
        print 'Submission table appended to.'
        print 'Collecting comments...'
        comment_table = self.build_comment_table()
        return submission_table, comment_table

if __name__ == "__main__":
  r = RabidReddit()
  sub_table, com_table = r.collect_all()

  pprint(sub_table)
  print
  print len(sub_table)
  print
  print
  print
  pprint(com_table)
  print
  print len(com_table)

# get_content
# Returns:	
# a list of reddit content, of type Subreddit, Comment, Submission or user flair.

# collect_all
# Returns:	
# request tables of submissions and comments 4 * 25 per request!

# comment table
#         return (
#             comment_id,
#             parent_id,
#             submission_id,
#             subreddit_id,
#             creation_date,
#             banned_by,
#             score,
#             gilded,
#             likes,
#             controversiality,
#             text)

# submission table
#         return (subreddit_id, 
#                 subreddit_name, 
#                 subscriber_count, 
#                 sub_id, 
#                 title, 
#                 author, 
#                 creation_date, 
#                 comment_count,  
#                 score, 
#                 nsfw)
