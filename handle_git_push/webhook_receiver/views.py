from django.shortcuts import render
from django.http import HttpResponse
import os

# Create your views here.


def handle_webhook(request):

    if request.method == 'POST':
        print('yyyy', request)
        print('xxxxxxx', request.POST)
        os.system('/root/fine_dust_pollution_visualization/handle_git_push/webhook_receiver/./script.sh')
        return HttpResponse('push found')
