from selenium.webdriver.support.ui import Select
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
drv = webdriver.Chrome('chromedriver.exe')
drv.get('https://www.google.ru/')
elm = drv.find_element_by_name('q')
elm.send_keys('selenide')
elm.send_keys(Keys.ENTER)

zzz = drv.find_elements_by_class_name("iUh30.Zu0yb.tjvcx")
#print(zzz[0].text)

if zzz[0].text == "https://ru.selenide.org":
    print("Первая ссылка - сайт selenide.org")
zzz = drv.find_element_by_xpath("//*[@id='hdtb-msb']/div[1]/div/div[3]/a")
zzz.click()

zzz = drv.find_element_by_xpath("//*[@id='islrg']/div[1]/div[1]/a[2]/div")
if "selenide" in zzz.text:
    print("Первое изображение selenide")

zzz = drv.find_element_by_xpath("//*[@id='yDmH0d']/div[2]/c-wiz/div[1]/div/div[1]/div[1]/div/div/a[1]")
zzz.click()

drv.close()

