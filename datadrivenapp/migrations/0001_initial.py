# Generated by Django 3.1.2 on 2020-10-29 10:00

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Agency',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('AGENCYID', models.IntegerField(null=True)),
                ('AGENCYCODE', models.CharField(max_length=50, null=True)),
                ('AGENCYNAME', models.CharField(max_length=500, null=True)),
                ('SECTOR', models.CharField(max_length=500, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='FromENGAS',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('AGENCYID', models.IntegerField(null=True)),
                ('BRANCHID', models.IntegerField(null=True)),
                ('TRANSACTION_NO', models.IntegerField(null=True)),
                ('JEV_NO', models.CharField(max_length=200, null=True)),
                ('PARTICULARS', models.TextField()),
                ('ENTRY_DATE', models.DateField(null=True)),
                ('AMOUNT', models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ('SUBSIDIARY_ACCOUNT', models.CharField(max_length=200, null=True)),
                ('MAJOR_ACCOUNT', models.CharField(max_length=200, null=True)),
                ('TRANSACTION_TYPE', models.CharField(max_length=200, null=True)),
                ('YEAR_ENTRY', models.IntegerField(null=True)),
                ('PAYEE', models.CharField(max_length=200, null=True)),
                ('DATE_UPLOADED', models.DateField(null=True)),
                ('REFERENCE_NUMBER', models.CharField(max_length=200, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='FromLBP',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('AGENCYID', models.IntegerField(null=True)),
                ('BRANCHID', models.IntegerField(null=True)),
                ('AGENCY', models.CharField(max_length=200, null=True)),
                ('ACCOUNT_NO', models.CharField(max_length=200, null=True)),
                ('CHECK_NO', models.CharField(max_length=200, null=True)),
                ('DATE', models.DateField(null=True)),
                ('PAYEE', models.CharField(max_length=200, null=True)),
                ('AMOUNT', models.DecimalField(decimal_places=2, max_digits=12, null=True)),
                ('DATE_UPLOADED', models.DateField(null=True)),
            ],
        ),
        migrations.CreateModel(
            name='MajorAccount',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ACCOUNT_TYPE_UID', models.CharField(max_length=200, null=True)),
                ('DESCRIPTION', models.CharField(max_length=200, null=True)),
                ('DEBIT_SL_FORM', models.CharField(max_length=200, null=True)),
                ('CREDIT_SL_FORM', models.CharField(max_length=200, null=True)),
                ('TRANSACTION_TYPE_UID', models.CharField(max_length=200, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='Transaction',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('TRANSACTION_TYPE_UID', models.CharField(max_length=200, null=True)),
                ('CODE', models.CharField(max_length=200, null=True)),
                ('DESCRIPTION', models.CharField(max_length=200, null=True)),
            ],
        ),
    ]
