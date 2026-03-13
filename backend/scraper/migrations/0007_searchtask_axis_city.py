from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('scraper', '0006_alter_globalsetting_value'),
    ]

    operations = [
        migrations.AddField(
            model_name='searchtask',
            name='city',
            field=models.CharField(blank=True, max_length=150),
        ),
        migrations.AddField(
            model_name='searchtask',
            name='axis',
            field=models.CharField(
                choices=[
                    ('lastname', 'Last Name'),
                    ('firstname', 'First Name'),
                    ('city', 'City'),
                    ('zip', 'ZIP Code'),
                ],
                default='lastname',
                max_length=20,
            ),
        ),
    ]
