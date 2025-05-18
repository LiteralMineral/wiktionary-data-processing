from django.db import models
from django.contrib.postgres.fields import ArrayField
# Create your models here.


class WordForm(models.Model):


    entry_id = models.BigIntegerField
    word = models.CharField(max_length=200, null=True)
    pos = models.CharField(max_length=50, null=True)
    form = models.CharField(max_length=200, null=True)
    head_nr = models.CharField(max_length=50, null=True)
    ipa = models.CharField(max_length=200, null=True)
    roman = models.CharField(max_length=200, null=True)
    source = models.CharField(max_length=200, null=True)
    tags = ArrayField(
        models.CharField(max_length= 100, null=True),
        size= 50,
        null=True
    )

    pass
