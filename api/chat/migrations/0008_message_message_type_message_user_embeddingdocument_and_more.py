# Generated by Django 4.1.7 on 2023-05-06 02:38

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    def update_message_user(apps, schema_editor):
        Message = apps.get_model('chat', 'message')
        Conversation = apps.get_model('chat', 'conversation')
        for message in Message.objects.all():
            conversation_id = message.conversation_id
            conversation_obj = Conversation.objects.get(id=conversation_id)
            user_id = conversation_obj.user_id
            if user_id:
                #print(f'message {message.id} to user {user_id}')
                message.user_id = user_id
                message.save()


    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('chat', '0007_message_messages_message_tokens'),
    ]

    operations = [
        migrations.AddField(
            model_name='message',
            name='message_type',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='message',
            name='user',
            field=models.ForeignKey(default=1, on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL),
            preserve_default=False,
        ),
        migrations.RunPython(update_message_user),
        migrations.AddField(
            model_name='message',
            name='is_disabled',
            field=models.BooleanField(default=False),
        ),
        migrations.CreateModel(
            name='EmbeddingDocument',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('faiss_store', models.BinaryField(null=True)),
                ('title', models.CharField(default='', max_length=255)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.AddField(
            model_name='message',
            name='embedding_message_doc',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='chat.embeddingdocument'),
        ),
    ]
