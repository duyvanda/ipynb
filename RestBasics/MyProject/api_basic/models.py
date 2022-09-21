from django.db import models

# Create your models here.

class Snippet(models.Model):
    title = models.CharField(max_length=100, blank=True, default='')
    author = models.CharField(max_length=100, blank=True, default='')
    email = models.EmailField(max_length=100, blank=True, default='')
    date = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return self.title
