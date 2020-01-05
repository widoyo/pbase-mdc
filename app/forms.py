from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, DecimalField
from wtforms import BooleanField, SubmitField, SelectField
from wtforms.validators import DataRequired

from app.models import Lokasi


class LoginForm(FlaskForm):
    username = StringField('Username', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    remember_me = BooleanField('Ingat saya')
    submit = SubmitField('Login')


class LokasiForm(FlaskForm):
    nama = StringField('Nama Lokasi', validators=[DataRequired()])
    ll = StringField('Longitude,Latitude', validators=[DataRequired()])
    submit = SubmitField('Tambah')


class LoggerSettingForm(FlaskForm):
    sn = StringField('Serial Number', validators=[DataRequired()])
    lokasi_id = SelectField('Lokasi', choices=[], coerce=int)
    temp_cor = DecimalField('Koreksi Suhu')
    humi_cor = DecimalField('Koreksi Kelemban')
    batt_cor = DecimalField('Koreksi Baterai')
    tipp_fac = DecimalField('Tipping Factor')
    ting_son = DecimalField('Tinggi Sonar (cm)')
    son_res = SelectField('Resolusi Sonar', choices=[(1000, 'Milimeter'),
                                                     (100, 'Centimeter')],
                                                     coerce=int)
