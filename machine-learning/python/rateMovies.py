import wx
import sys
from os import remove
from os.path import dirname, join, isfile
from time import time
import wx.lib.scrolledpanel as scrolled
########################################################################

topMovies = """1, Toy Story (1995)
780, Independence Day (a.k.a. ID4) (1996)
590, Dances with Wolves (1990)
1210, Star Wars: Episode VI - Return of the Jedi (1983)
648, Mission: Impossible (1996)
344, Ace Ventura: Pet Detective (1994)
165, Die Hard: With a Vengeance (1995)
153, Batman Forever (1995)
597, Pretty Woman (1990)
1580, Men in Black (1997)
231, Dumb & Dumber (1994)
914, My Fair Lady (1964)
121308, Goodbye to Language 3D (2014)
131796, Woman in Gold (2015)
115713, Ex Machina (2015)"""
topMoviesLines = topMovies.split("\n")

class MovieRatingFrame(wx.Frame):
    """"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        wx.Frame.__init__(self, None, title="Rate Movie",  size=(768, 500))
        self.panel = wx.Panel(self)
        self.count = 0
        self.currentMovie = topMoviesLines[self.count]

        self.mainSizer = wx.BoxSizer(wx.VERTICAL)
        
        msgTxt = "Please rate the following movie (1-5 (best), or 0 if not seen): "
        heading = wx.StaticText(self.panel, label=msgTxt, style=wx.ALIGN_CENTRE)
        self.mainSizer.Add(heading, flag=wx.ALL, border=5)

        infoTxt = "You have %s tries left"%(str(len(topMoviesLines)-self.count))
        self.message = wx.StaticText(self.panel, label=infoTxt, style=wx.ALIGN_CENTRE)
        self.mainSizer.Add(self.message, flag=wx.ALL, border=5)        

        movieLbl = wx.StaticText(self.panel, label="Movie:")
        self.movie = wx.TextCtrl(self.panel, style=wx.TE_READONLY)
        self.movie.SetValue(self.currentMovie)
        self.addWidgets(movieLbl, self.movie)

        ratingLbl = wx.StaticText(self.panel, label="Rating:")
        self.rating = wx.TextCtrl(self.panel)
        self.rating.SetValue("0")
        self.addWidgets(ratingLbl, self.rating)

        successMsg = ""
        self.successLbl = wx.StaticText(self.panel, label=successMsg, style=wx.ALIGN_CENTRE)
        self.mainSizer.Add(self.successLbl, flag=wx.ALL, border=5)

        btn = wx.Button(self.panel, label="Ok")
        btn.Bind(wx.EVT_BUTTON, self.onClick)
        self.mainSizer.Add(btn, 0, wx.ALL|wx.CENTER, 5)

        outputMsg = ""
        self.outputLabel = wx.StaticText(self.panel, label=outputMsg, style=wx.ALIGN_CENTRE)
        self.mainSizer.Add(self.outputLabel, flag=wx.ALL, border=5)
        self.openFile()


    #----------------------------------------------------------------------
    def addWidgets(self, lbl, txt):
        """"""
        sizer = wx.BoxSizer(wx.HORIZONTAL)
        sizer.Add(lbl, 0, wx.ALL|wx.CENTER, 5)
        sizer.Add(txt, 1, wx.ALL|wx.EXPAND, 5)
        self.mainSizer.Add(sizer, 0, wx.ALL|wx.EXPAND)
    #--------------------------------------------------------------------
    def openFile(self):
        parentDir = dirname(dirname(__file__))
        self.ratingsFile = join(parentDir, "../personalRatings.txt")

        if isfile(self.ratingsFile):
            r = raw_input("Looks like you've already rated the movies. Overwrite ratings (y/N)? ")
            if r and r[0].lower() == "y":
                remove(self.ratingsFile)
                self.panel.SetSizer(self.mainSizer)
                self.Show()
            else:
                self.getRec()
                self.Hide()
        self.file = open(self.ratingsFile, 'w')


    #----------------------------------------------------------------------
    def onClick(self, event):
        """"""
        userRating = str(self.rating.GetValue())
        movie = self.currentMovie

        now = int(time())

        if self.count < len(topMoviesLines)-1:
            # count for the number of tries 
            # add movie to file
            ls = movie.strip().split(",")
            valid = False
            rStr = userRating
            r = int(rStr) if rStr.isdigit() else -1
            print(type(r), r)
            if r < 0 or r > 5:
                self.successLbl.SetLabel("Invalid, enter again")
                valid = True
            else:
                self.count += 1
                self.successLbl.SetLabel("Successfull Added")
                valid = True
                if r > 0:
                    self.file.write("0::%s::%d::%d\n" % (ls[0], r, now))
            # reset form
            self.currentMovie = topMoviesLines[self.count]
            self.movie.SetValue(self.currentMovie)
            self.rating.SetValue("0")
            message = "You have %s tries left"%(str(len(topMoviesLines)-self.count))
            self.message.SetLabel(message)
        else:
            self.file.close()
            self.Hide()
            self.Destroy()
            self.getRec()

    def getRec(self):
        # -------------------------------------------------------

        text = self.getDesc()
        app3 = wx.App(0)
        newFrame2 = NewFrame(None)
        fa2 = MovieRecommenderPanel(newFrame2, text)
        newFrame2.Show()
        app3.MainLoop()
        # # -------------------------------------------------------

    def getDesc(self):
        from MovieLensALS import getRecommendation
        arg0 = "MovieLensALS.py"
        arg1 = "/home/hduser/Downloads/spark-training-master/data/movielens/small/"
        arg2 = "../personalRatings.txt"
        outputBuffer = getRecommendation(arg0, arg1, arg2)
        return outputBuffer

#----------------------------------------------------------------------
class MovieRecommenderPanel(scrolled.ScrolledPanel):

    def __init__(self, parent, text):

        scrolled.ScrolledPanel.__init__(self, parent, -1,  size=(768, 500))

        vbox = wx.BoxSizer(wx.VERTICAL)
        self.loadingLabel = wx.StaticText(self, -1, "loading... ")
        vbox.Add(self.loadingLabel, 0, wx.ALIGN_LEFT | wx.ALL, 5)

        if text:
            self.removeLoading()
            self.desc = wx.StaticText(self, -1, text)
            self.desc.SetForegroundColour("Blue")
            vbox.Add(self.desc, 0, wx.ALIGN_LEFT | wx.ALL, 5)
            vbox.Add(wx.StaticLine(self, -1, size=(1024, -1)), 0, wx.ALL, 5)
            vbox.Add((20, 20))

        self.SetSizer(vbox)
        self.SetupScrolling()

    def removeLoading(self):
        self.loadingLabel.SetLabel("")


class NewFrame(wx.Frame):
           
    def __init__(self, *args, **kw):
        super(NewFrame, self).__init__( size=(768, 500), *args, **kw)
        vbox = wx.BoxSizer(wx.VERTICAL)
        self.loadingLabel = wx.StaticText(self, -1, "loading... ")
        vbox.Add(self.loadingLabel, 0, wx.ALIGN_LEFT | wx.ALL, 5)

        self.InitUI()
                
    def InitUI(self):
        self.Bind(wx.EVT_CLOSE, self.OnCloseWindow)

        self.SetTitle('Movies Recommended for you')
        self.Centre()
        self.Show(True)

    def OnCloseWindow(self, e):

        dial = wx.MessageDialog(None, 'Are you sure to quit?', 'Question',
            wx.YES_NO | wx.NO_DEFAULT | wx.ICON_QUESTION)
            
        ret = dial.ShowModal()
        
        if ret == wx.ID_YES:
            self.Destroy()
        else:
            e.Veto()
        sys.exit()


if __name__ == "__main__":
    app = wx.App(False)
    frame = MovieRatingFrame()
    app.MainLoop()
