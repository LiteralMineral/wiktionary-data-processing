# Generated by Django 5.2.1 on 2025-05-14 20:44

import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("main_app", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="wordform",
            name="form",
            field=models.CharField(max_length=200, null=True),
        ),
        migrations.AddField(
            model_name="wordform",
            name="head_nr",
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AddField(
            model_name="wordform",
            name="ipa",
            field=models.CharField(max_length=200, null=True),
        ),
        migrations.AddField(
            model_name="wordform",
            name="pos",
            field=models.CharField(max_length=50, null=True),
        ),
        migrations.AddField(
            model_name="wordform",
            name="roman",
            field=models.CharField(max_length=200, null=True),
        ),
        migrations.AddField(
            model_name="wordform",
            name="source",
            field=models.CharField(max_length=200, null=True),
        ),
        migrations.AddField(
            model_name="wordform",
            name="tags",
            field=django.contrib.postgres.fields.ArrayField(
                base_field=models.CharField(max_length=100, null=True),
                null=True,
                size=50,
            ),
        ),
        migrations.AlterField(
            model_name="wordform",
            name="word",
            field=models.CharField(max_length=200, null=True),
        ),
    ]
