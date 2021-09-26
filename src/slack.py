from requests import post
from random import randint

channel_to_post_to = "sea_level_rise"

greetings = ["Kia ora!", "Howdy partner.", "G'day mate.", "What up g? :sunglasses:", "Ugh finally, this shit is done.", "Kachow! :racing_car:", "Kachigga! :racing_car:", "Sup dude.", "Woaaaaahhhh, would you look at that!", "Easy peasy.", "Rock on bro. :call_me_hand:", "Leshgoooooo!", "Let's get this bread!", "You're doing great dude. :kissing_heart:", "Another one bits the dust...", "Sup, having a good day?", "Yeeeeeeehaw cowboy! :face_with_cowboy_hat:"]  


def post_message_to_slack(message, greet=True):
    """
    Function to post a message to a Slack channel.
    
    :param message: A string to post to the slack channel.
    :param greet: Default: True. If True, post a cheerful greeting before the message.
    :returns: None.

    """
    
    if greet:
        greeting = greetings[randint(0, max(len(greetings) - 1, 0))] + " "
    else:
        greeting = ""

    post('https://slack.com/api/chat.postMessage', {
        'token': open('./config/slack_token.txt', 'r').read().strip('\n'),
        'channel': '#' + channel_to_post_to,
        'text': greeting + message
    })